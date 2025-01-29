create table source as remote(
    '{{.CH_SOURCE_HOST}}', 
    {{.CH_SOURCE_DATABASE | default "default"}}, 
    {{.CH_SOURCE_TABLE}},
    '{{.CH_SOURCE_USER | default "default"}}',
    '{{.CH_SOURCE_PASSWD | default ""}}'
)
