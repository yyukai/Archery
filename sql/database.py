# -*- coding: UTF-8 -*-
import datetime
import simplejson as json

from django.db.models import Q
from sql.utils.permission import get_ding_user_id_by_permission
from sql.utils.ding_api import DingSender
from sql.utils.resource_group import user_instances
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse

from common.utils.extend_json_encoder import ExtendJSONEncoder
from .models import Instance, DataBase, Replication


@permission_required('sql.menu_database', raise_exception=True)
def db_list(request):
    res = {}
    if request.method == 'GET':
        limit = int(request.GET.get('limit'))
        offset = int(request.GET.get('offset'))
        instance_name = request.GET.get('instance_name', '')
        search = request.GET.get('search', '')
        try:
            if instance_name:
                obj_list = DataBase.objects.filter(instance__instance_name=instance_name).filter(
                    Q(db_name__contains=search) | Q(db_application__contains=search) |
                    Q(db_person__contains=search)).distinct()
            else:
                obj_list = DataBase.objects.filter(Q(db_name__contains=search) |
                                                   Q(db_application__contains=search) |
                                                   Q(db_person__contains=search)).order_by('instance').distinct()
            res = list()
            for obj in obj_list[offset:offset + limit]:
                res.append({
                    'id': obj.id,
                    'ip_port': '{}:{}'.format(obj.instance.host, obj.instance.port) if obj.instance else "未分配",
                    'instance': obj.instance.instance_name if obj.instance else "未分配",
                    'db_name': obj.db_name,
                    'db_application': obj.db_application,
                    'db_person': obj.db_person,
                })
            res = {"total": obj_list.count(), "rows": res}
        except Instance.DoesNotExist:
            res = {'status': 1, 'msg': 'Instance.DoesNotExist'}
        except Exception as e:
            res = {'status': 1, 'msg': str(e)}
    if request.method == 'POST':
        db_name = request.POST.get('db_name', '')
        app_type = request.POST.get('app_type', '')
        db_application = request.POST.get('db_application', '')
        db_person = request.user.display
        try:
            if DataBase.objects.filter(db_name=db_name).exists():
                instance_name = DataBase.objects.get(db_name=db_name).instance_name
                raise Exception("数据库：{} 已存在！实例名：{}".format(db_name, instance_name))
            db = DataBase.objects.create(db_name=db_name, app_type=app_type, db_application=db_application,
                                         db_person=db_person)
            res = {'status': 0, 'msg': 'ok'}
            msg = '申请新增数据库：\n数据库：{}\n业务：{}\n用途：{}\n申请人：{}\n地址：{}\n请您尽快补全或删除该数据库信息！'.format(db_name,
                    app_type, db_application, db_person, "http://dbms.weidai.com.cn/admin/sql/database/%s/change/" % db.id)
            ding_sender = DingSender()
            ding_user_ids = get_ding_user_id_by_permission('database_edit')
            for ding_id in ding_user_ids:
                ding_sender.send_msg(ding_id, msg)
        except Exception as e:
            res = {'status': 1, 'msg': str(e)}
    return HttpResponse(json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@permission_required('sql.menu_database', raise_exception=True)
def db_detail(request):
    instance_name = request.POST.get('instance_name', '')
    search = request.POST.get('search', '')
    try:
        if instance_name:
            obj_list = DataBase.objects.filter(instance__instance_name=instance_name).filter(
                Q(db_name__contains=search) | Q(db_application__contains=search) | Q(db_person__contains=search))
        else:
            obj_list = DataBase.objects.filter(Q(db_name__contains=search) |
                                               Q(db_application__contains=search) |
                                               Q(db_person__contains=search))
        res = list()
        for obj in obj_list:
            res.append({
                'id': obj.id,
                'host': obj.instance.host,
                'ip_port': '{}:{}'.format(obj.instance.host, obj.instance.port),
                'instance': obj.instance.instance_name,
                'db_name': obj.db_name,
                'db_application': obj.db_application,
                'db_person': obj.db_person,
            })
    except Instance.DoesNotExist:
        print('Instance.DoesNotExist')
        res = {'status': 1, 'msg': 'Instance.DoesNotExist'}
    except Exception as e:
        print(e)
        res = {'status': 1, 'msg': str(e)}
    return HttpResponse(json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@csrf_exempt
@permission_required('sql.menu_instance', raise_exception=True)
def replication_delay(request):
    """
        instance_info = [
            [ins1_id, ins1_ip, ins1],
            [ins2_id, ins2_ip, ins2],
            ......
            [ins6_id, ins6_ip, ins6]
        ]
        delay_info = {
            ins1_id: [[ins2, 0], [ins3, 0]],
            ins5_id: [[ins6, 0]]
        }
        :param request:
        :return:
        """
    masters = list()
    delay_info = {}
    all_instances = list()
    for ins in user_instances(request.user, type='all', db_type=['mysql']):
        all_instances.append([str(ins.id), ins.host + ":" + str(ins.port), ins.instance_name])
    hour = datetime.datetime.now() - datetime.timedelta(hours=1)
    ins_name = request.GET.get('name', '')
    if ins_name:
        for ins in Instance.objects.filter(instance_name=ins_name):
            masters.append([str(ins.id), ins.host + ":" + str(ins.port), ins.instance_name])
    else:
        for ins in user_instances(request.user, type='master', db_type=['mysql']):
            masters.append([str(ins.id), ins.host + ":" + str(ins.port), ins.instance_name])

    for ins in user_instances(request.user, type='all', db_type=['mysql']):
        all_instances.append([str(ins.id), ins.host + ":" + str(ins.port), ins.instance_name])
        slave_ins_info = list()
        for slave in Instance.objects.filter(parent=ins, type='slave'):
            rep = Replication.objects.filter(master=ins.instance_name, slave=slave.instance_name, created__gte=hour)
            slave_ins_info.append([str(slave.id), rep[0].delay if rep else 'NaN'])
        delay_info[str(ins.id)] = slave_ins_info
    res = {'instance_info': all_instances, 'masters': masters, 'delay_info': delay_info}
    return HttpResponse(json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
