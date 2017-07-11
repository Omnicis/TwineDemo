# Project Stage and Package Structure

As defined in pom.xml, project name space is `com.omnicis.mag`, and project name is **Medical Affairs Group Project**.

## SmvApp and Stages

New SMV App-Module framework introduced a new intermediate layer called **Stage**. Each **SmvApp** could have multiple **Stages**, and each **Stage** has one Scala **Packages** and
one sub-package with name `input`.

For each project, the App-Stage-Package hierarchy is defined under `conf` dir, typically in `smv-app-conf.props` file.

For this project the structure is defined in `conf` file as the following,
```
# stage definitions
smv.stages = com.omnicis.commondata.pdda.geo, com.omnicis.commondata.pdda.seer, com.omnicis.mag.pdda.cms, com.omnicis.mag.pdda.geoprof, com.omnicis.mag.pdda.phys, com.omnicis.mag.lung.internal, com.omnicis.mag.lung.cci
```

The dir structure under `/src/main/scala` is the following:
```
com/
└── omnicis
    └── mag
        ├── adhoc
        │   └── Census_2012.scala
        ├── core
        │   ├── CensusDemoMap.scala
        │   ├── LookUpMap.scala
        │   ├── LungConf.scala
        │   ├── LungRecode.scala
        │   └── NameMatch.scala
        ├── lung
        │   ├── cci
        │   │   ├── IncRateSeer.scala
        │   │   ├── README.md
        │   │   └── input
        │   │       └── package.scala
        │   └── internal
        │       ├── README.md
        │       ├── Sales.scala
        │       ├── Sha.scala
        │       └── input
        │           └── package.scala
        └── pdda
            ├── README.md
            ├── cms
            │   ├── CMS.scala
            │   └── input
            │       └── package.scala
            ├── geoprof
            │   ├── Census2012Data.scala
            │   ├── Chr.scala
            │   ├── README.md
            │   ├── StateCancerProfile.scala
            │   └── input
            │       └── package.scala
            └── phys
                ├── NPPES.scala
                ├── Referral.scala
                └── input
                    └── package.scala
```

**Note** the `conf` file actually has 2 external stages listed
* `com.omnicis.commondata.pdda.geo`
* `com.omnicis.commondata.pdda.seer`

They are in the `TwineCommonData` project and will be used as input to `LungCancer`
project.

At the `mag` level, the 5 stages are `lung/cci`, `lung/internal`, `pdda/cms`,
`pdda/geoprof`, `pdda/phys`. Each of them has a sub-package `input`.

Other than the 5 stages, there are 2 more directories: adhoc and core. Both of them are packages, where `core` has all the shared tools, and `adhoc` is a sandbox to do adhoc analysis. Typically `adhoc` package can be dropped entirely without impacting the App output.

The `input` sub-package of each stage defines all the links to the input `SmvFile`s or
`SmvModuleLink`s.
* `SmvFile` defines access to the data file
* `SmvModuleLink` defines links to `SmvModule` in other **Stages**
