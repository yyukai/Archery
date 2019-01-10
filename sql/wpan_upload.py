# -*- coding: UTF-8 -*-
import datetime
import os
import re
import time
import datetime
import logging
import simplejson as json
from wsgiref.util import FileWrapper
from django.db.models import Q
from django.http import HttpResponse, JsonResponse
from sql.models import Config, WPanHistory
from common.utils.extend_json_encoder import ExtendJSONEncoder
from common.utils.file_api import get_file_content
from sql.utils.wpan_api import WPan

logger = logging.getLogger('default')


def file_path_auditor(user, file_path):
    root_path = Config.objects.get(item='wpan_upload_dir').value
    if not re.match(root_path, file_path):
        return False, '根目录设置有误！'
    if not user.is_superuser and not user.has_perm('sql.wpan_upload_audit'):
        re_path = os.path.join(root_path, user.username)
        if not re.match(re_path, file_path):
            return False, '只允许查看自己的文件！'
    return True, ""


def wpan_upload_dir_list(request):
    result = list()
    try:
        wpan_upload_dir = Config.objects.get(item='wpan_upload_dir')
        if not os.path.exists(wpan_upload_dir.value):
            os.mkdir(wpan_upload_dir.value)
        if request.user.is_superuser:
            wpan_upload_dir_path = wpan_upload_dir.value
            if not os.path.exists(os.path.join(wpan_upload_dir_path, request.user.username)):
                os.mkdir(os.path.join(wpan_upload_dir_path, request.user.username))
        else:
            wpan_upload_dir_path = os.path.join(wpan_upload_dir.value, request.user.username)
        if not os.path.exists(wpan_upload_dir_path):
            os.mkdir(wpan_upload_dir_path)
        generator = os.walk(wpan_upload_dir_path)
        for parent, dir_names, _ in sorted(generator, key=lambda key: key[0]):
            for name in sorted(dir_names):
                result.append({'id': os.path.join(parent, name), 'name': name + "/", 'parent_id': parent})
    except Config.DoesNotExist:
        return JsonResponse({"code": 1, "errmsg": "未配置云盘根目录！"}, safe=True)
    except Exception as e:
        return JsonResponse({"code": 1, "errmsg": "异常：%s" % str(e)}, safe=True)
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


def wpan_upload_list(request):
    if request.method == 'GET':
        result = list()
        try:
            path = request.GET.get('path')
            if path is None or not os.path.exists(path):
                JsonResponse(result)
            stat, output = file_path_auditor(request.user, path)
            if stat is False:
                return JsonResponse(result)

            search = request.GET.get('search', '')
            for f in os.listdir(path):
                if search not in f:
                    continue
                file_path = os.path.join(path, f)
                state = os.stat(file_path)
                mtime = time.strftime("%Y-%m-%d %X", time.localtime(state.st_mtime))
                if os.path.isfile(file_path):
                    size = round(state.st_size / float(1024), 2)
                    result.append({"type": "file", "size": size, "mtime": mtime, "name": f, "path": file_path})
                else:
                    result.append({"type": "dir", "size": "-", "mtime": mtime, "name": f, "path": file_path})
        except Exception as e:
            print(e)
        return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                            content_type='application/json')
    if request.method == 'POST':
        # 上传文件
        import_files = request.FILES.getlist('files', None)
        if len(import_files) == 0:
            return JsonResponse({'code': 1, 'errmsg': '上传错误！'})

        root_path = Config.objects.get(item='wpan_upload_dir')
        parent_dir = os.path.join(root_path.value, request.user.username, datetime.datetime.now().strftime("%Y%m%d"))
        if not os.path.exists(parent_dir):
            os.mkdir(parent_dir)
        created, failed = [], []
        for im_file in import_files:
            file_path = os.path.join(parent_dir, im_file.name)
            with open(file_path, 'wb') as f:
                for chunk in im_file.chunks():
                    f.write(chunk)
            if not os.path.isfile(file_path):
                failed.append('{}：文件为空，或上传错误！'.format(im_file.name))
                continue
            created.append('{}：上传成功！'.format(im_file.name))

        data = {
            'code': 0,
            'created': created,
            'created_info': 'Created {}'.format(len(created)),
            'failed': failed,
            'failed_info': 'Failed {}'.format(len(failed)),
            'msg': 'Created: {}, Error: {}'.format(len(created), len(failed))
        }
        return HttpResponse(json.dumps(data, cls=ExtendJSONEncoder, bigint_as_string=True),
                            content_type='application/json')


def wpan_upload_file_cont(request):
    file_path = request.GET.get('path')
    stat, output = file_path_auditor(request.user, file_path)
    if stat is False:
        return HttpResponse(output)
    if os.path.isfile(file_path):
        file_content = get_file_content(file_path, request.user.username)
    else:
        return HttpResponse("该文件不存在！")
    return HttpResponse(file_content.replace('\n', '</br>'))


def wpan_upload_apply(request):
    file_path = request.GET.get('path')
    reason = request.GET.get('reason')
    stat, output = file_path_auditor(request.user, file_path)
    if stat is False:
        return JsonResponse({"total": 1, "result": output})
    if os.path.isfile(file_path):
        WPanHistory.objects.create(apply=request.user, file_path=file_path, reason=reason, status=0)
        result = {"total": 0, "result": "申请已提交！请等待管理员审核！"}
    else:
        result = {"total": 1, "result": "该文件不存在！请联系管理员查看详情！"}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


def wpan_upload_download(request):
    file_path = request.GET.get("path")
    status, output = file_path_auditor(request.user, file_path)
    if status is False:
        return HttpResponse(output)
    if os.path.isfile(file_path):
        wrapper = FileWrapper(open(file_path, "rb"))
        response = HttpResponse(wrapper, content_type='application/octet-stream')
        response['Content-Length'] = os.path.getsize(file_path)
        response['Content-Disposition'] = 'attachment; filename="%s"' % file_path.split('/')[-1]
        return response
    else:
        return HttpResponse("文件不存在！")


def wpan_upload_audit(request):
    if request.method == 'GET':
        # 72小时前的申请，视为已过期申请
        three_day_ago = datetime.datetime.now() - datetime.timedelta(days=3)
        WPanHistory.objects.filter(status=0, create_time__lte=three_day_ago).update(status=3)

        search = request.GET.get('search', '')
        if search:
            obj_list = WPanHistory.objects.filter(Q(apply__username=search) | Q(auditor__username=search) |
                                                  Q(file_path__contains=search) |
                                                  Q(reason__contains=search)).distinct().order_by('status', '-create_time')
        else:
            obj_list = WPanHistory.objects.get_queryset().order_by('status', '-create_time')
        result = list()
        for obj in obj_list:
            result.append({
                'id': obj.id,
                'apply': obj.apply.username,
                'auditor': obj.auditor.username if obj.auditor else "",
                'file_name': obj.file_path.split('/')[-1],
                'file_path': obj.file_path,
                'reason': obj.reason,
                'error_msg': obj.error_msg,
                'audit_msg': obj.audit_msg,
                'status': obj.status,
                'create_time': obj.create_time
            })
    if request.method == 'POST':
        apply_id = request.POST.get('apply_id', '')
        audit_msg = request.POST.get('audit_msg', '')
        is_allow = request.POST.get('is_allow', '')
        wpan_apply = WPanHistory.objects.get(id=apply_id)
        if is_allow == "no":
            wpan_apply.audit_msg = audit_msg
            wpan_apply.status = 2
            wpan_apply.save(update_fields=['audit_msg', 'status'])
            result = {"total": 0, "result": "申请已打回！"}
        else:
            # 推送文件到微贷云盘
            pan = WPan(file=wpan_apply.file_path)
            ret = pan.upload_small_files()
            file_id = ret['file_id']
            pan.create_user_share(file_id=file_id, expired='', user_ding_id=request.user.ding_user_id)

            result = {"total": 0, "result": "文件正在推送中，请过一会后检查云盘空间！"}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
