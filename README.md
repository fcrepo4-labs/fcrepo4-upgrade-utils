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
Usage: fcrepo-upgrade-utils [-h] [-d=<arg>] [-i=<arg>]
                            [--migration-user=<arg>]
                            [--migration-user-address=<arg>]
                            [-o=<arg>] [-p=<arg>] [-r=<arg>]
                            [-s=<arg>] [-t=<arg>] [-u=<arg>]
  -h, --help      Show this help message and exit.
  -d,--digest-algorithm=<arg>
                  The digest algorithm to use in OCFL.
                    Default: sha512
  -i,--input-dir=<arg>
                  The path to the directory containing
                    a Fedora 4.7.x or Fedora 5.x export
  --migration-user=<arg>
                  The user to attribute OCFL versions to. 
                    Default: fedoraAdmin
  --migration-user-address=<arg>
                  The address of the user OCFL versions are 
                  attributed to. 
                    Default: info:fedora/fedoraAdmin
  -o,--output-dir=<arg>
                  The path to the directory where upgraded 
                  resources will be written.
                    Default value: output_<yyyyMMdd-HHmmss>
  -p,--threads=<arg>
                  The number of threads to use.
                    Default: the number of available cores
  -r,--source-rdf=<arg>
                  The RDF language used in the Fedora export. 
                    Default: Turtle
  -s,--source-version=<arg>
                  The version of Fedora that was the source 
                  of the export. 
                    Valid values: 5+,4.7.5
  -t,--target-version=<arg>
                  The version of Fedora to which you are upgrading. 
                    Valid values: 5+,6+
  -u,--base-uri=<arg>
                  Fedora's base URI. 
                    For example, http://localhost:8080/rest
```

Building
--------

To build the JAR file

``` sh
mvn package
```
