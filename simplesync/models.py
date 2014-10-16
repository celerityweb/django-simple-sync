# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import functools

logger = logging.getLogger(__name__)

import json

import django
from django.db.models import signals
from django.core.serializers import serialize, deserialize
from django.core.serializers.json import DateTimeAwareJSONEncoder


def fail_silently(fn):
    @functools.wraps(fn)
    def __wrapper_(*args, **kwargs):
        try:
            fn(*args, **kwargs)
        except Exception, e:
            logger.exception('Failure in signal handler')
    return __wrapper_


class ModelSyncer(object):

    def __init__(self, model):
        self.model = model

    def get_model_name(self, model_cls):
        if django.VERSION < (1,6):
            return model_cls._meta.module_name
        else:
            return model_cls._meta.model_name

    def uses_natural_key(self, obj):
        return hasattr(obj, 'natural_key') and callable(obj.natural_key) and \
               hasattr(obj._default_manager, 'get_by_natural_key') and \
               callable(obj._default_manager.get_by_natural_key)

    def pk_or_nk(self, obj):
        return obj.natural_key() if self.uses_natural_key(obj) else obj.pk

    @fail_silently
    def pre_save_or_delete_handler(self, sender=None, instance=None, raw=None, using=None,
                                   update_fields=None, **kwargs):
        """This is necessary to track the primary key prior to save. Especially
        in some unusual circumstances, a primary or even a natural key might
        change. We need to know about it."""
        if raw:
            logger.debug('Pre-save/delete declining to run for raw save.')
        # A model instance has a ModelState instance which we can use and then
        # piggyback onto.
        if instance.pk is not None:
            # This object has a primary key defined on it.
            # We should see if this is an update or if it's an insert.
            try:
                original_obj = sender._default_manager.using(using).get(pk=instance.pk)
            except sender.DoesNotExist:
                # This will be a create
                pass
            else:
                instance._state.original_key = self.pk_or_nk(instance)

    @fail_silently
    def post_save_handler(self, sender=None, instance=None, created=None,
                          raw=None, using=None, update_fields=None, **kwargs):
        from . import tasks
        if raw:
            logger.warning('Received "raw" save request for %s %s - declining '
                           'to operate', self.get_model_name(sender), instance.pk)
            return
        if created:
            if not self.can_create(instance):
                logger.debug('Received create signal for %s %s - but not '
                             'authorized by can_create',
                             self.get_model_name(sender), instance.pk)
                return
            result = tasks.do_sync.delay('create',
                                         sender._meta.app_label,
                                         self.get_model_name(sender),
                                         None,  # original_key
                                         self.to_json(instance))
            logger.info('CREATE - %s %s - queued as %s',
                        self.get_model_name(sender), instance.pk, result.id)
            return
        else:
            if not self.can_update(instance):
                logger.debug('Received update signal for %s %s - but not '
                             'authorized by can_update',
                             self.get_model_name(sender), instance.pk)
                return
            result = tasks.do_sync.delay('update',
                                         sender._meta.app_label,
                                         self.get_model_name(sender),
                                         instance._state.original_key,
                                         self.to_json(instance))
            logger.info('UPDATE - %s %s - queued as %s',
                        self.get_model_name(sender), instance.pk, result.id)
            return

    @fail_silently
    def post_delete_handler(self, sender=None, instance=None, using=None,
                            **kwargs):
        from . import tasks
        if not self.can_delete(instance):
            logger.debug('Received delete signal for %s %s - but not '
                         'authorized by can_delete',
                         self.get_model_name(sender), instance.pk)
            return
        json_body = {'pk': instance._state.original_key}
        result = tasks.do_sync.delay(
            'delete', sender._meta.app_label, self.get_model_name(sender), None,
            json.dumps(json_body, cls=DateTimeAwareJSONEncoder))
        logger.info('DELETE - %s %s - queued as %s',
                    self.get_model_name(sender), json_body, result.id)

    @fail_silently
    def m2m_changed_handler(self, sender=None, instance=None, action=None,
                            reverse=None, model=None, pk_set=None,
                            using=None, **kwargs):
        # instance is what model's m2m (or reverse m2m) is being modified
        if type(instance) != self.model:
            return

        ThroughClass = sender
        syncer = type(self)(ThroughClass)
        instance_model_name = self.get_model_name(type(instance))

        from . import tasks
        if action == 'post_add' and self.can_add_m2m(type(instance), model):
            # Treat this like a create
            for pk in pk_set:
                obj = ThroughClass.objects.get(
                    **{instance_model_name: instance,
                       self.get_model_name(model): pk}
                )
                # We need a JSON generator with this model now...
                result = tasks.do_sync.delay('create',
                                             sender._meta.app_label,
                                             self.get_model_name(ThroughClass),
                                             None,  # original_key
                                             syncer.to_json(obj))
                logger.info('CREATE - %s %s - queued as %s',
                            self.get_model_name(ThroughClass), instance.pk, result.id)
            return

        if action == 'post_remove' and \
                self.can_remove_m2m(type(instance), model):
            for pk in pk_set:
                json_body = {instance_model_name: self.pk_or_nk(instance)}
                related_obj = model.objects.get(pk=pk)
                json_body[self.get_model_name(model)] = self.pk_or_nk(related_obj)
                result = tasks.do_sync.delay(
                    'delete', ThroughClass._meta.app_label,
                    self.get_model_name(ThroughClass),
                    None, json.dumps(json_body, cls=DateTimeAwareJSONEncoder))
                logger.info('DELETE - %s %s - queued as %s',
                            self.get_model_name(sender), json_body, result.id)

        if action == 'pre_clear' and \
                self.can_remove_m2m(type(instance), model):
            # We only want to send delete signals for those ThroughClass instances
            # that existed, not just a blanket "delete all of them"
            # so we have to track which ones existed.
            model_name = self.get_model_name(model)
            m2m_qs = ThroughClass.objects.filter(
                **{instance_model_name: instance.pk}).select_related(model_name)
            instance._state.m2m_clear_pks = [
                {instance_model_name: self.pk_or_nk(instance),
                 model_name: self.pk_or_nk(getattr(m2m_obj, model_name))}
                for m2m_obj in m2m_qs]

        if action == 'post_clear' and \
                self.can_remove_m2m(type(instance), model):
            for json_body in instance._state.m2m_clear_pks:
                result = tasks.do_sync.delay(
                    'delete', sender._meta.app_label, self.get_model_name(ThroughClass),
                    None, json.dumps(json_body, cls=DateTimeAwareJSONEncoder))
                logger.info('DELETE - %s %s - queued as %s',
                            self.get_model_name(ThroughClass), json_body, result.id)

    def can_add_m2m(self, model, other_model):
        return is_registered(model) and is_registered(other_model)

    def can_remove_m2m(self, model, other_model):
        return is_registered(model) and is_registered(other_model)

    def can_create(self, obj):
        return True

    def can_update(self, obj):
        return True

    def can_delete(self, obj):
        return True

    def to_json(self, obj):
        return serialize('json', [obj], use_natural_keys=True)

    def from_json(self, json_obj):
        logger.debug('json_obj: %s', json_obj)
        deserialized_obj = deserialize('json', json_obj).next()
        return deserialized_obj.object, deserialized_obj.m2m_data


class SyncerRegistry(object):
    def __init__(self):
        self.registered = {}

    def register(self, model, cls):
        if model in self.registered:
            logger.info('Model %s is already registered for syncing',
                        self.get_model_name(model))
            return
        instance = cls(model)
        signals.pre_save.connect(instance.pre_save_or_delete_handler, sender=model)
        signals.pre_delete.connect(instance.pre_save_or_delete_handler, sender=model)
        signals.post_save.connect(instance.post_save_handler, sender=model)
        signals.post_delete.connect(instance.post_delete_handler,
                                    sender=model)
        signals.m2m_changed.connect(instance.m2m_changed_handler)
        self.registered[model] = instance

__registry__ = SyncerRegistry()
is_registered = lambda model_cls: model_cls in __registry__.registered
