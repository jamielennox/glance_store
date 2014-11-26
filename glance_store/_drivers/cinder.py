#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Storage backend for Cinder"""

import logging

from cinderclient import exceptions as cinder_exception
from cinderclient.v2 import client as cinderclient
from keystoneclient import exceptions as ks_exceptions
from keystoneclient import session
from oslo.config import cfg

from glance_store.common import utils
import glance_store.driver
from glance_store import exceptions
from glance_store.i18n import _
import glance_store.location

LOG = logging.getLogger(__name__)

_CINDER_OPTS = [
    cfg.StrOpt('cinder_catalog_info',
               default='volume:cinder:publicURL',
               help='Info to match when looking for cinder in the service '
                    'catalog. Format is : separated values of the form: '
                    '<service_type>:<service_name>:<endpoint_type>'),
    cfg.StrOpt('cinder_endpoint_template',
               default=None,
               help='Override service catalog lookup with template for cinder '
                    'endpoint e.g. http://localhost:8776/v1/%(project_id)s'),
    cfg.StrOpt('os_region_name',
               default=None,
               help='Region name of this node'),
    cfg.IntOpt('cinder_http_retries',
               default=3,
               help='Number of cinderclient retries on failed http calls'),
]


_CINDER_OPTS.extend([session.Session.get_conf_options(deprecated_opts={
    'cafile': [cfg.DeprecatedOpt('cinder_ca_certificates_file')],
    'insecure': [cfg.DeprecatedOpt('cinder_api_insecure')]
})])


class StoreLocation(glance_store.location.StoreLocation):

    """Class describing a Cinder URI."""

    def process_specs(self):
        self.scheme = self.specs.get('scheme', 'cinder')
        self.volume_id = self.specs.get('volume_id')

    def get_uri(self):
        return "cinder://%s" % self.volume_id

    def parse_uri(self, uri):
        if not uri.startswith('cinder://'):
            reason = _("URI must start with 'cinder://'")
            LOG.info(reason)
            raise exceptions.BadStoreUri(message=reason)

        self.scheme = 'cinder'
        self.volume_id = uri[9:]

        if not utils.is_uuid_like(self.volume_id):
            reason = _("URI contains invalid volume ID")
            LOG.info(reason)
            raise exceptions.BadStoreUri(message=reason)


class Store(glance_store.driver.Store):

    """Cinder backend store adapter."""

    OPTIONS = _CINDER_OPTS
    EXAMPLE_URL = "cinder://<VOLUME_ID>"

    def __init__(self, conf):
        super(Store, self).__init__(conf)
        self.session = session.Session.load_from_conf_options(conf,
                                                              'glance_store')

        info = conf.glance_store.cinder_catalog_info.split(':')
        self.service_type, self.service_name, self.endpoint_type = info

    def get_schemes(self):
        return ('cinder',)

    def _check_context(self, context):
        """
        Configure the Store to use the stored configuration options
        Any store that needs special configuration should implement
        this method. If the store was not able to successfully configure
        itself, it should raise `exceptions.BadStoreConfiguration`
        """

        if context is None:
            reason = _("Cinder storage requires a context.")
            raise exceptions.BadStoreConfiguration(store_name="cinder",
                                                   reason=reason)

    def _get_cinderclient(self, context):
        endpoint_override = self.conf.glance_store.cinder_endpoint_template
        if endpoint_override:
            endpoint_override = endpoint_override % context.to_dict()

        auth_plugin = context.get_auth_plugin()

        return cinderclient.Client(
            session=self.session,
            auth=auth_plugin,
            region_name=self.conf.glance_store.os_region_name,
            retries=self.conf.glance_store.cinder_http_retries)

    def get_size(self, location, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file and returns the image size

        :param location `glance_store.location.Location` object, supplied
                        from glance_store.location.get_location_from_uri()
        :raises `glance_store.exceptions.NotFound` if image does not exist
        :rtype int
        """

        loc = location.store_location

        try:
            self._check_context(context)
            volume = self._get_cinderclient(context).volumes.get(loc.volume_id)
            # GB unit convert to byte
            return volume.size * (1024 ** 3)
        except cinder_exception.NotFound as e:
            reason = _("Failed to get image size due to "
                       "volume can not be found: %s") % self.volume_id
            LOG.error(reason)
            raise exceptions.NotFound(reason)
        except ks_exceptions.EndpointNotFound:
            reason = _("Cinder endpoint could not be determined")
            raise exceptions.BadStoreConfiguration(store_name="cinder",
                                                   reason=reason)
        except Exception as e:
            LOG.exception(_("Failed to get image size due to "
                            "internal error: %s") % e)
            return 0
