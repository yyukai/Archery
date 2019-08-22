# -*- coding: UTF-8 -*-
import re
import os
import MySQLdb
import datetime
import simplejson as json
from django.db.models import Q
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, JsonResponse
from django.contrib.auth.decorators import permission_required
from django.shortcuts import render
from common.utils.extend_json_encoder import ExtendJSONEncoder
from sql.utils.api import BASE_DIR
from sql.models import ToolsLoanUpdate
# from sql.utils.ding_api import DingSender
#
# ding_sender = DingSender()
# ml_db_host, ml_db_port, ml_db_user, ml_db_pwd = "192.168.16.250", 6606, "uatloan", "UATloan123!"
# au_db_host, au_db_port, au_db_user, au_db_pwd = "192.168.16.250", 6606, "uatauth", "UATauth123!"
ml_db_host, ml_db_port, ml_db_user, ml_db_pwd = "172.20.2.7", 3338, "wdsfangjb", "WDsfangjb1016!"
au_db_host, au_db_port, au_db_user, au_db_pwd = "172.20.2.7", 3336, "wdsfangjb", "WDsfangjb1016!"


@permission_required('sql.tools_loan_update', raise_exception=True)
def tools_loan_update(request):
    return render(request, 'tools_loan_update.html')


def loan_update_search(request):
    loan_id = request.GET.get('loan_id', None)
    emp_id = request.GET.get('t_sale_id', None)
    username = request.GET.get('username', None)

    if not loan_id or not re.match(r'10\d+', emp_id, re.I):
        return JsonResponse({"code": -1, "errmsg": "异常调用！"})

    db1 = MySQLdb.connect(host=ml_db_host, port=ml_db_port, user=ml_db_user, passwd=ml_db_pwd, db="loan",
                          charset='utf8', use_unicode=True)
    c1 = db1.cursor()
    sql1 = """SELECT b.id,b.`sale_id`,b.`sale_man` FROM main_loan_order b WHERE b.id IN ('{}');""".format(loan_id)
    c1.execute(sql1)
    tuple_order = c1.fetchall()
    db2 = MySQLdb.connect(host=au_db_host, port=au_db_port, user=au_db_user, passwd=au_db_pwd, db="auth",
                          charset='utf8', use_unicode=True)
    c2 = db2.cursor()
    if emp_id:
        sql2 = """SELECT username,realname,empid_new,daishuuid FROM user WHERE empid_new IN ('{}');""".format(emp_id)
    else:
        sql2 = """SELECT username,realname,empid_new,daishuuid FROM user WHERE username IN ('{}');""".format(username)
    c2.execute(sql2)
    tuple_user = c2.fetchall()

    result = list()
    if len(tuple_user) == 0:
        for o in tuple_order:
            result.append({
                "t_sale_id": "查不到",
                "t_sale_name": "查不到",
                "t_emp_id": emp_id,
                "t_sale_uid": "查不到",
                "loan_id": loan_id,
                "s_sale_id": o[1],
                "s_sale_name": o[2]
            })
    else:
        for o in tuple_order:
            result.append({
                "t_sale_id": tuple_user[0][0],
                "t_sale_name": tuple_user[0][1],
                "t_emp_id": emp_id,
                "t_sale_uid": tuple_user[0][3],
                "loan_id": loan_id,
                "s_sale_id": o[1],
                "s_sale_name": o[2]
            })
    # 返回查询结果
    res = {"code": 0, "data": result}
    return HttpResponse(json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
def loan_update_apply(request):
    loan_id = request.POST.get('loan_id', '')
    s_sale_id = request.POST.get('s_sale_id', '')
    s_sale_name = request.POST.get('s_sale_name', '')
    t_sale_id = request.POST.get('t_sale_id', '')
    t_sale_name = request.POST.get('t_sale_name', '')
    t_emp_id = request.POST.get('t_emp_id', '')
    t_sale_uid = request.POST.get('t_sale_uid', '')
    import_files = request.FILES.getlist('files', [])
    if len(import_files) == 0:
        return JsonResponse({'code': -1, 'errmsg': '文件上传错误！'})

    home_path = os.path.join(os.path.basename(BASE_DIR), "static", "loan_update")
    if not os.path.exists(home_path):
        os.makedirs(home_path)
    pic_name_list = list()
    for im_file in import_files:
        file_name = datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '-' + im_file.name
        file_path = os.path.join(home_path, file_name)
        with open(file_path, 'wb') as f:
            for chunk in im_file.chunks():
                f.write(chunk)
        pic_name_list.append(file_name)

    ToolsLoanUpdate.objects.create(loan_id=loan_id, s_sale_id=s_sale_id, s_sale_name=s_sale_name, t_sale_id=t_sale_id,
                                   t_sale_name=t_sale_name, t_emp_id=t_emp_id, t_sale_uid=t_sale_uid,
                                   t_pic_name="|".join(pic_name_list), applicant=request.user)
    return JsonResponse({"code": 0, "result": "后台已记录，请通知相关人员审核确认！"})


@csrf_exempt
def loan_update_apply_edit(request):
    lu_id = request.POST.get('id', -1)
    # loan_id = request.POST.get('loan_id', '')
    # s_sale_id = request.POST.get('s_sale_id', '')
    # s_sale_name = request.POST.get('s_sale_name', '')
    # t_sale_id = request.POST.get('t_sale_id', '')
    # t_sale_name = request.POST.get('t_sale_name', '')
    # t_emp_id = request.POST.get('t_emp_id', '')
    # t_sale_uid = request.POST.get('t_sale_uid', '')
    import_files = request.FILES.getlist('files', [])

    if not ToolsLoanUpdate.objects.filter(id=lu_id).exists():
        return JsonResponse({'code': -1, 'errmsg': '非法调用！'})
    else:
        tlu = ToolsLoanUpdate.objects.get(id=lu_id)
        if tlu.status != 0:
            return JsonResponse({'code': -1, 'errmsg': '只允许修改待审核状态的申请！'})
    if len(import_files) == 0:
        return JsonResponse({'code': -1, 'errmsg': '文件上传错误！'})

    home_path = os.path.join(os.path.basename(BASE_DIR), "static", "loan_update")
    if not os.path.exists(home_path):
        os.makedirs(home_path)
    pic_name_list = list()
    for im_file in import_files:
        file_name = datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '-' + im_file.name
        file_path = os.path.join(home_path, file_name)
        with open(file_path, 'wb') as f:
            for chunk in im_file.chunks():
                f.write(chunk)
        pic_name_list.append(file_name)

    ToolsLoanUpdate.objects.filter(id=lu_id).update(t_pic_name="|".join(pic_name_list), applicant=request.user)
    # ToolsLoanUpdate.objects.filter(id=lu_id).update(loan_id=loan_id, s_sale_id=s_sale_id, s_sale_name=s_sale_name,
    #                                                 t_sale_id=t_sale_id, t_sale_name=t_sale_name, t_emp_id=t_emp_id,
    #                                                 t_sale_uid=t_sale_uid, t_pic_name="|".join(pic_name_list),
    #                                                 applicant=request.user)
    # 返回查询结果
    return JsonResponse({"code": 0, "result": "修改成功，请耐心等待管理员审核！"})


@permission_required('sql.tools_loan_update', raise_exception=True)
def tools_loan_update_audit(request):
    return render(request, 'tools_loan_update_audit.html')


def loan_update_audit_list(request):
    limit = int(request.GET.get('limit'))
    offset = int(request.GET.get('offset'))
    limit = offset + limit

    # 72小时前的申请，视为已过期申请
    three_day_ago = datetime.datetime.now() - datetime.timedelta(days=3)
    # for tlu in ToolsLoanUpdate.objects.filter(status=0, create_time__lte=three_day_ago):
    #     msg_content = """您的订单业务员变更申请： {}\n订单编号：{}\n原业务员：{}\n修正业务员：{}\n""".format(
    #         tlu.get_status_display(), tlu.loan_id, tlu.s_sale_id, tlu.t_sale_id)
    #     ding_sender.send_msg_sync(tlu.applicant.ding_user_id, msg_content)
    ToolsLoanUpdate.objects.filter(status=0, create_time__lte=three_day_ago).update(status=3)

    search = request.GET.get('search', '')
    if request.user.has_perm('sql.tools_loan_update_audit'):
        if search:
            obj_list = ToolsLoanUpdate.objects.filter(Q(loan_id=search) | Q(s_sale_id=search) | Q(s_sale_name=search) |
                                                      Q(t_sale_id=search) | Q(t_sale_name=search))
        else:
            obj_list = ToolsLoanUpdate.objects.get_queryset()
    else:
        if search:
            obj_list = ToolsLoanUpdate.objects.filter(Q(loan_id=search) | Q(s_sale_id=search) |
                                                      Q(s_sale_name=search) | Q(t_sale_id=search) |
                                                      Q(t_sale_name=search)).filter(applicant=request.user).distinct()
        else:
            obj_list = ToolsLoanUpdate.objects.filter(applicant=request.user)
    obj_count = obj_list.count()

    # QuerySet 序列化
    rows = [row for row in obj_list[offset:limit].values("id", "loan_id", "s_sale_id", "s_sale_name", "t_sale_id",
                                                         "t_sale_name", "t_emp_id", "t_sale_uid",
                                                         "applicant__display", "audit_msg", "auditor__display",
                                                         "status", "create_time")]
    result = {"total": obj_count, "rows": rows}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@permission_required('sql.tools_loan_update', raise_exception=True)
def loan_update_pic_list(request):
    pic_url_list = list()
    lu_id = request.GET.get('id', -1)
    if not ToolsLoanUpdate.objects.filter(id=lu_id).exists():
        return JsonResponse({"code": -1, "result": "申请不存在！"})
    else:
        lu = ToolsLoanUpdate.objects.get(id=lu_id)
        if not request.user.has_perm('sql.tools_loan_update_audit') and lu.applicant != request.user:
            return JsonResponse({"code": -1, "result": "您无查看权限！"})

        home_dir = os.path.join("/static", "loan_update")
        for pic_name in lu.t_pic_name.split("|"):
            if pic_name:
                pic_url_list.append({"name": pic_name, "uri": os.path.join(home_dir, pic_name)})
    res = {"code": 0, "result": pic_url_list}
    return HttpResponse(json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
def loan_update_audit(request):
    lu_id = request.POST.get('id', -1)
    action = request.POST.get('action', None)
    if ToolsLoanUpdate.objects.filter(id=lu_id).exists():
        if action == "cancel":
            ToolsLoanUpdate.objects.filter(id=lu_id).update(status=4)
            return JsonResponse({"code": 0, "result": "您的申请已经取消！"})
        else:
            if not request.user.has_perm('sql.tools_loan_update_audit'):
                return JsonResponse({"code": -1, "result": "您无审核权限！"})
            audit_msg = request.POST.get('audit_msg', "")
            if action == "pass":
                ToolsLoanUpdate.objects.filter(id=lu_id).update(status=1, auditor=request.user, audit_msg=audit_msg)
                return JsonResponse({"code": 0, "result": "该申请已审核通过！"})
            elif action == "reject":
                ToolsLoanUpdate.objects.filter(id=lu_id).update(status=2, auditor=request.user, audit_msg=audit_msg)
                return JsonResponse({"code": 0, "result": "该申请已拒绝！"})
            else:
                JsonResponse({"code": -1, "result": "非法调用！"})
            # tlu = ToolsLoanUpdate.objects.get(id=lu_id)
            # msg_content = """您的订单业务员变更申请： {}\n订单编号：{}\n审核人：{}\n理由：{}\n""".format(
            #     tlu.get_status_display(), tlu.loan_id, tlu.auditor.display, tlu.audit_msg)
            # ding_sender.send_msg_sync(tlu.applicant.ding_user_id, msg_content)
    return JsonResponse({"code": -1, "result": "非法调用！"})
