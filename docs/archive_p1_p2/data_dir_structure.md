# Config `smv.dataDir`

One can specify the data dir in a user global setting or a project specific setting:
* $HOME/.smv/smv-user-conf.props
* conf/smv-user-conf.props

The content of the file should be
```
smv.dataDir = /path/to/data
```

The `/path/to/data` part should be replaced by your data dir absolute path.

# Structure in data dir
```
data/
├──input
|  ├── pdda_raw
|  └── Sha_Mock_data
└──output
```

where `pdda_raw` and `Sha_Mock_data` should be downloaded from the data server by
following the Confluence page

https://omnicis.atlassian.net/wiki/display/ADMIN/Data+Server+Data+Sync
