"""datarelay URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path

import futures.views
import secgov.views
import assets.views
import idx.views
import crypto.views

urlpatterns = [
    path('sec.gov/forms', secgov.views.get_sec_gov_forms),
    path('assets', assets.views.get_assets),
    path('crypto', crypto.views.get_crypto_series),
    path('data', assets.views.get_data),
    path('idx/list', idx.views.get_idx_list),
    path('idx/data', idx.views.get_idx_data),
    path('futures/list', futures.views.get_list),
    path('futures/data', futures.views.get_data),


    path('last/<str:last_time>/sec.gov/forms', secgov.views.get_sec_gov_forms),
    path('last/<str:last_time>/assets', assets.views.get_assets),
    path('last/<str:last_time>/crypto', crypto.views.get_crypto_series),
    path('last/<str:last_time>/data', assets.views.get_data),
    path('last/<str:last_time>/idx/list', idx.views.get_idx_list),
    path('last/<str:last_time>/idx/data', idx.views.get_idx_data),
    path('futures/<str:last_time>/list', futures.views.get_list),
    path('futures/<str:last_time>/data', futures.views.get_data),
    # path('admin/', admin.site.urls),
]
