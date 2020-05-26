from invoke import task

s3_bucket='library.metatab.org'
wp_site='data.sandiegodata.org'

groups = ['Health']
tags = ['county','national']

group_flags = ' '.join([ f"-g{g}" for g in groups])
tag_flags = ' '.join([ f"-t{t}" for t in tags])

def force_flag(force):
    return '-F' if force else ''

wp_flags = f' -w {wp_site} {group_flags} {tag_flags}' if wp_site else ''
s3_flags = f' -s {s3_bucket}' if s3_bucket else ''

@task
def make(c, force=False):
    """Build, write to S3, and publish to wordpress, but only if necessary"""
    c.run(f'mp -q  make {force_flag(force)} -r  -b {s3_flags} {wp_flags}' )

@task
def build(c, force=False):
    c.run(f"mp build -r {force_flag(force)}")
    
@task
def publish(c):
    if s3_bucket:
        c.run(f"mp s3 -s {s3_bucket}")
    if wp_site:
        c.run(f"mp wp -s {wp_site} {group_flags} {tag_flags} -p")