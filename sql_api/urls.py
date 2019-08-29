from django.urls import path
from sql_api.views.info import info
from sql_api.views.agent import instance_edit, host_edit, db_agent

urlpatterns = [
    path('info', info),
    path('v1/instance/edit/', instance_edit),
    path('v1/host/edit/', host_edit),
    path('v1/db_agent/', db_agent),
]
