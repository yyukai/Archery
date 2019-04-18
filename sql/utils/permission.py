# -*- coding: UTF-8 -*-
from django.contrib.auth.models import Permission


def get_ding_user_id_by_permission(code):
    ding_user_id_list = list()
    for p in Permission.objects.filter(codename=code):
        for g in p.group_set.all():
            for u in g.user_set.all():
                ding_user_id_list.append(u.ding_user_id)
    return list(set(ding_user_id_list))
