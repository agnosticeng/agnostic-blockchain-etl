create table sink as remote(
    '{{.CH_SINK_HOST}}', 
    {{.CH_SINK_DATABASE | default "default"}}, 
    {{.CH_SINK_TABLE}},
    '{{.CH_SINK_USER | default "default"}}',
    '{{.CH_SINK_PASSWD | default ""}}'
)
