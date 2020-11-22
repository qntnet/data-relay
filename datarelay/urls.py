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

import cryptofutures.views
import futures.views
import secgov.views
import assets.views
import idx.views
import crypto.views
import blsgov.views

urlpatterns = [
    path('sec.gov/forms', secgov.views.get_sec_gov_forms),
    path('sec.gov/facts', secgov.views.request_facts),

    path('assets', assets.views.get_assets),
    path('data', assets.views.get_data),

    path('crypto', crypto.views.get_crypto_series),
    path('cryptofutures', crypto.views.get_crypto_series),

    path('idx/list', idx.views.get_idx_list),
    path('idx/data', idx.views.get_idx_data),

    path('major-idx/list', idx.views.get_major_list),
    path('major-idx/data', idx.views.get_major_data),

    path('futures/list', futures.views.get_list),
    path('futures/data', futures.views.get_data),

    path('bls.gov/db/list', blsgov.views.get_dbs),
    path('bls.gov/db/meta', blsgov.views.get_db_meta),
    path('bls.gov/series/list', blsgov.views.get_series_meta),
    path('bls.gov/series/data', blsgov.views.get_series_data),
    path('bls.gov/series/aspect', blsgov.views.get_series_aspect),

    path('last/<str:last_time>/bls.gov/db/list', blsgov.views.get_dbs),
    path('last/<str:last_time>/bls.gov/db/meta', blsgov.views.get_db_meta),
    path('last/<str:last_time>/bls.gov/series/list', blsgov.views.get_series_meta),
    path('last/<str:last_time>/bls.gov/series/data', blsgov.views.get_series_data),
    path('last/<str:last_time>/bls.gov/series/aspect', blsgov.views.get_series_aspect),

    path('last/<str:last_time>/sec.gov/forms', secgov.views.get_sec_gov_forms),
    path('last/<str:last_time>/sec.gov/facts', secgov.views.request_facts),

    path('last/<str:last_time>/assets', assets.views.get_assets),
    path('last/<str:last_time>/data', assets.views.get_data),

    path('last/<str:last_time>/cryptofutures', cryptofutures.views.get_cryptofutures_series),

    path('last/<str:last_time>/idx/list', idx.views.get_idx_list),
    path('last/<str:last_time>/idx/data', idx.views.get_idx_data),

    path('last/<str:last_time>/major-idx/list', idx.views.get_major_list),
    path('last/<str:last_time>/major-idx/data', idx.views.get_major_data),

    path('last/<str:last_time>/futures/list', futures.views.get_list),
    path('last/<str:last_time>/futures/data', futures.views.get_data),
    # path('admin/', admin.site.urls),
]
