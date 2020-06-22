# Task definitions for invoke
# You must first install invoke, https://www.pyinvoke.org/

# You can also create you own tasks
from invoke import task

from metapack_build.tasks.package import ns

# To configure options for invoke functions you can:
# - Set values in the 'invoke' section of `~/.metapack.yaml
# - Use one of the other invoke config options:
#   http://docs.pyinvoke.org/en/stable/concepts/configuration.html#the-configuration-hierarchy
# - Set the configuration in this file:

# ns.configure(
#    {
#        'metapack':
#            {
#                's3_bucket': 'bucket_name',
#                'wp_site': 'wp sot name',
#                'groups': None,
#                'tags': None
#            }
#    }
# )

# However, the `groups` and `tags` hould really be set in the `metatada.csv`
# file, and `s3_bucket` and `wp_site` should be set at the collection or global level


@task
def example_task(c):
    """An exmaple Invoke task"""
    c.run("echo 'this is an example task' ")


ns.add_task(example_task)
