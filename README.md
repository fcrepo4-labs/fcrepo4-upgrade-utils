Fedora 4 Utilities
==================

Utilities for maintaining the [Fedora Commons repository](http://github.com/fcrepo4/fcrepo4).

* TechnicalMetadataMigrator: migrate technical metadata properties based on changes made in May 2015:
    * fedora:digest => premis:hasMessageDigest
    * fedora:mimeType => ebucore:hasMimeType
    * premis:hasOriginalName => ebucore:filename

Building
--------

To build the JAR file

``` sh
mvn package
```

Running
-------

To run the migration utility, execute the JAR file and provide the `fcrepo.home` system property to set the directory that contains Fedora 4's `fcrepo4-data` directory.

``` sh
java -Dfcrepo.home=/path/to/fcrepo.home -jar /path/to/fcrepo4-upgrade-utils/target/fcrepo-upgrade-utils-4.3.1-SNAPSHOT.jar
```

To run the migration utility in "dry-run" mode where it will output a summary of the migration it would perform, but not actually change the repository: 

``` sh
java -Dfcrepo.home=/path/to/fcrepo.home -jar /path/to/fcrepo4-upgrade-utils/target/fcrepo-upgrade-utils-4.3.1-SNAPSHOT.jar dryrun
```
