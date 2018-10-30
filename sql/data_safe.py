# -*- coding: UTF-8 -*-

import simplejson as json
import logging
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse
from django.db.models import Q
from sql.utils.group import user_instances
from sql.models import DataMaskingColumns, DataMaskingRules
from common.utils.extend_json_encoder import ExtendJSONEncoder

logger = logging.getLogger('default')


@permission_required('sql.masking_field', raise_exception=True)
def masking_field_list(request):
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit

    obj_list = user_instances(request.user, 'all')
    db_name_list = [n.instance_name for n in obj_list]

    rows = list()
    search = request.POST.get('search', '')
    if search:
        obj_list = DataMaskingColumns.objects.filter(instance_name__in=db_name_list).\
            filter(Q(table_schema__contains=search) | Q(table_name__contains=search) | Q(column_name__contains=search))
    else:
        obj_list = DataMaskingColumns.objects.filter(instance_name__in=db_name_list)

    for dmc in obj_list[offset:limit]:
        dmr = DataMaskingRules.objects.filter(rule_type=dmc.rule_type)
        rule_regex = dmr[0].rule_regex if dmr else '-'
        hide_group = dmr[0].hide_group if dmr else '-'
        rule_desc = dmr[0].rule_desc if dmr else '-'
        rows.append({'id': dmc.column_id, 'rt': dmc.get_rule_type_display(), 'act': dmc.get_active_display(),
                     'ins': dmc.instance_name, 'db': dmc.table_schema, 'tb': dmc.table_name, 'cn': dmc.column_name,
                     'cc': dmc.column_comment, 'rr': rule_regex, 'hg': hide_group, 'rd': rule_desc, 'time': dmc.create_time})

    result = {"total": len(rows), "rows": rows}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
