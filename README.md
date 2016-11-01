Fedora 4 Upgrade Utilities
==========================

Utilities for version upgrades of the [Fedora Commons repository](http://github.com/fcrepo4/fcrepo4).

Upgrading from 4.3, 4.5, 4.6 to Fedora 4.7.

### Prerequisites
* Java 8
   
### Utilities
* BackupFixer:
    * This utility is needed in the event the /fcr:backup process outputs duplicate resources, each sharing the same identifier/key.
    * Warning: This utility is destructive (but likely not in a bad way), and should be run against a copy of the backup if the backup is irreplaceable.
    * It should be run on the directory produced by a /fcr:backup request, *if necessary*, prior to requesting /fcr:restore.
    * For details on executing /fcr:backup and /fcr:restore, see [documentation](https://wiki.duraspace.org/display/FEDORA47/RESTful+HTTP+API+-+Backup+and+Restore)
   * **Running**
      * You should first try to backup/restore your repository without using this utility.
         1. Backup your repository:  ```
        curl -X POST $HOST:$PORT/$CONTEXT/rest/fcr:backup ```
         2. Try to restore your repository: ```
        curl -X POST --data-binary "/tmp/fcrepo4-data/path/to/backup/directory" $HOST:$PORT/$CONTEXT/rest/fcr:restore ```
      * If Step 2 results in an error similar to those documented in this [ticket](https://jira.duraspace.org/browse/FCREPO-2069), then you will need to run the BackupFixer utility.
         
         ```
         java -jar /path/to/fcrepo4-upgrade-utils/target/fcrepo-upgrade-utils-4.8.0-SNAPSHOT.jar /path/to/backup/directory
         ```
         Once this completes, re-try step 2.
         
### Building
To build the JAR file

``` sh
mvn package
```

### Earlier Version Upgrades
Upgrading from 4.2 to Fedora 4.3

* See [documentation](https://github.com/fcrepo4-exts/fcrepo4-upgrade-utils/tree/4.2-4.3)

