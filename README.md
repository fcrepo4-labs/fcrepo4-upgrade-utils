fcrepo-upgrade-utils [![Build Status](https://github.com/fcrepo-exts/fcrepo-upgrade-utils/workflows/Build/badge.svg)](https://github.com/fcrepo-exts/fcrepo-upgrade-utils/actions)
==================

Utility for upgrading the [Fedora Commons repository](http://github.com/fcrepo/fcrepo).

Usage
-----

General usage is as follows:

```sh
java -jar target/fcrepo-upgrade-utils-<version>.jar [cli options | help]
```

The following CLI options are available:

```
usage: java -jar fcrepo-upgrade-util-<version>.jar
 -d,--digest-algorithm <arg>         The digest algorithm to use in OCFL.
                                     Default: sha512
 -h,--help                           Print these options
 -i,--input-dir <arg>                The path to the directory containing
                                     a Fedora 4.7.x or Fedora 5.x export
    --migration-user <arg>           The user to attribute OCFL versions
                                     to. Default: fedoraAdmin
    --migration-user-address <arg>   The address of the user OCFL versions
                                     are attributed to. Default:
                                     info:fedora/fedoraAdmin
 -o,--output-dir <arg>               The path to the directory where
                                     upgraded resources will be written.
                                     Default value:
                                     output_<yyyyMMdd-HHmmss>. For
                                     example: output_20200101-075901
 -p,--threads <arg>                  The number of threads to use.
                                     Default: the number of available
                                     cores
 -r,--source-rdf <arg>               The RDF language used in the Fedora
                                     export. Default: Turtle
 -R,--resource-info-file <arg>       The path of the file that contains a
                                     list of resources to be processed
    --skip-acls                      Skip creating fcr:acl resources
                                     and migrate webac:Acl and
                                     acl:Authorization resources normally
 -s,--source-version <arg>           The version of Fedora that was the
                                     source of the export. Valid values:
                                     5+,4.7.5
    --s3-access-key <arg>            The AWS access key, optionally use
                                     when writing to S3
    --s3-bucket <arg>                The S3 bucket to write to, required
                                     when writing to S3
    --s3-endpoint <arg>              The AWS endpoint URL, optionally use
                                     when writing to S3
    --s3-path-style-access           The S3 access style, optionally use
                                     when writing to S3
    --s3-prefix <arg>                The S3 prefix to locate the OCFL repo
                                     in, optionally use when writing to S3
    --s3-region <arg>                The AWS region, optionally use when
                                     writing to S3
    --s3-secret-key <arg>            The AWS secret key, optionally use
                                     when writing to S3
 -t,--target-version <arg>           The version of Fedora to which you
                                     are upgrading. Valid values: 5+,6+
 -u,--base-uri <arg>                 Fedora's base URI. For example,
                                     http://localhost:8080/rest
    --write-to-s3                    Enables writing migrated Fedora 6
                                     data to S3 rather than the local
                                     filesystem
```

### Resuming Fedora 6 Migration

If a Fedora 6 migration is interrupted or a subset of resources fail to migrate, then a log file named
`remaining_TIMESTAMP.log` is created that contains information about the resources that were not migrated. The
migration can be resumed by passing this file back to the utility on a subsequent run. For example:

```shell
java -jar fcrepo-upgrade-utils.jar \
  --input-dir my-5.1.1-export \
  --source-version 5+ \
  --target-version 6+ \
  --base-uri http://localhost:8080/rest \
  --resource-info-file remaining_TIMESTAMP.log
```

### S3

If you are migrating to Fedora 6 and you want your migrated data to live in S3, then you can configure the utility
to write directly to S3. To do so, you must at the minimum specify `--write-to-s3` and `--s3-bucket`. Specifying
`--s3-prefix` is recommended as otherwise the migrated OCFL repository will use the entire bucket. All of the other
S3 related options can be derived from [standard AWS configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
or specified explicitly.

Building
--------

To build the JAR file

``` sh
mvn package
```
