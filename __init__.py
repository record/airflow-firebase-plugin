import random
from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
import firebase_admin
import firebase_admin.credentials
import firebase_admin.ml


class FirebaseHook(BaseHook):
    def __init__(self, firebase_conn_id='firebase_default'):
        self.firebase_conn_id = firebase_conn_id
        self.firebase_app = None


    def _get_app(self):
        if self.firebase_app is not None:
            return self.firebase_app

        conn_object = self.get_connection(self.firebase_conn_id)
        firebase_app_name = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for _ in range(8))
        firebase_cert = conn_object.password
        firebase_bucket = conn_object.extra_dejson['storage_bucket']

        self.firebase_app = firebase_admin.initialize_app(
            credentials.Certificate(firebase_cert),
            options={'storageBucket': firebase_bucket},
            name = firebase_app_name,
        )
        return self.firebase_app


    def _get_model(self, model_name):
        model_filter = 'display_name: {}'.format(model_name)

        for model in firebase_admin.ml.list_models(list_filter=model_filter, app=self._get_app()).iterate_all():
            print 'model id: {}'.format(model.model_id)
            print 'model info: {}'.format(model.as_dict())
            return model

        return None


    def put_model(self, model_name, model_filepath, tags):
        model_source = firebase_admin.ml.TFLiteGCSModelSource.from_tflite_model_file(model_filepath, app=self._get_app())
        model_tflite = firebase_admin.ml.TFLiteFormat(model_source=source)
        model = self._get_model(model_name)

        if model:
            new_model = firebase_admin.mlml.update_model(model=model, app=self._get_app())
        else:
            new_model = firebase_admin.ml.create_model(
                model=firebase_admin.ml.Model(
                    display_name=model_name,
                    tags=tags,
                    model_format=model_tflite,
                ),
                app=self._get_app()
            )

        firebase_admin.ml.publish_model(model_id=new_model.model_id, app=self._get_app())
        return new_model.as_dict()


class S3ToFirebaseOperator(BaseOperator):
    template_fields = ('aws_conn_id',
                       'firebase_conn_id',
                       'model_tags',
                       's3_prefix',
                       's3_key')
    template_ext = tuple()


    @apply_defaults
    def __init__(self,
                 aws_conn_id='aws_default',
                 firebase_conn_id='firebase_default',
                 model_tags=[],
                 s3_bucket=None,
                 s3_key=None,
                 *argv, **kwargs):
        super(S3ToFirebaseOperator, self).__init__(*argv, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.firebase_conn_id = firebase_conn_id
        self.model_tags = model_tags
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key


    def execute(self, context):
        model_name = os.path.split(s3_key)[1]
        self.log.info('Model name: %s', model_name)

        s3_hook = S3Hook(self.aws_conn_id)
        firebase_hook = FirebaseHook(self.firebase_conn_id)

        with TemporaryDirectory(prefix='airflow_firebaseop_') as tmp_dir:
            with NamedTemporaryFile(model_name, dir=tmp_dir, delete=False) as tmp:
                self.log.info('Download s3://%s/%s', self.s3_bucket, self.s3_key)

                s3_obj = s3_hook.get_key(self.s3_key, self.s3_bucket)
                s3_obj.download_fileobj(tmp)

                model_filepath = tmp.name
                self.log.info('Model file: %s', model_filepath)

            self.log.info('Create/Update model')
            model_info = firebase_hook.put_model(self, model_name, model_filepath, self.model_tags)

        self.log.info('Model info: %s', model_info)
        return model_info


class FirebasePlugin(AirflowPlugin):
    name = "firebase_plugin"
    hooks = [FirebaseHook]
    operators = [S3ToFirebaseOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
