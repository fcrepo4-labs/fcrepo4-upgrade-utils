/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.upgrade.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.riot.RDFLanguages;

import java.io.File;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.fcrepo.upgrade.utils.Config.VALID_DIGEST_ALGORITHMS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author dbernstein
 * @since 2019-08-05
 */
public class UpgradeUtilDriver {

    private static final org.slf4j.Logger logger = getLogger(UpgradeUtilDriver.class);

    public static final Map<FedoraVersion, Set<FedoraVersion>> VALID_MIGRATION_PATHS = new HashMap<>();

    static {
        VALID_MIGRATION_PATHS.put(FedoraVersion.V_4_7_5, Set.of(FedoraVersion.V_5));
        VALID_MIGRATION_PATHS.put(FedoraVersion.V_5, Set.of(FedoraVersion.V_6));
    }

    private UpgradeUtilDriver() {
        // Prevent public instantiation
    }

    /**
     * The main entry point
     *
     * @param args from the command line
     */
    public static void main(final String[] args) {
        final UpgradeUtilDriver driver = new UpgradeUtilDriver();

        try {
            driver.run(args);

        } catch (final Exception e) {
            logger.error("Error performing upgrade: {}", e.getMessage());
            logger.debug("Stacktrace: ", e);
        }
    }

    private void run(final String[] args) {
        final var configOptions = options();
        final var config = parseOptions(configOptions, args);

        try {
            //start the upgrade
            final var manager = UpgradeManagerFactory.create(config);
            manager.start();
        } catch (final Exception e) {
            logger.error("Upgrade failed.", e);
            printHelpAndExit(format("Upgrade failed: %s  -> See log for details.", e.getMessage()), configOptions);
        }
    }

    private Config parseOptions(final Options configOptions, final String[] args) {
        // first see if they've specified a config file
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd;
        try {
            cmd = parser.parse(configOptions, args);
        } catch (final ParseException e) {
            printHelpAndExit(e.getMessage(), configOptions);
            throw new RuntimeException("I am unreachable");
        }

        logger.info("Command line parameters: ");
        for(final var option : cmd.getOptions()) {
            logger.info("    {}: {}", option.getLongOpt(), option.getValues());
        }


        final File inputDir = new File(cmd.getOptionValue("i"));

        if (!inputDir.exists()) {
            printHelpAndExit(format("input directory %s does not exist.", inputDir.getAbsolutePath()), configOptions);
        }

        final var commandArgs = cmd.getArgs();

        if(commandArgs.length > 0) {
            final var errorMessage =
                new StringBuilder(format("The following argument(s) were not expected: %s .", commandArgs));
            errorMessage.append("  Please ensure all arguments are associated with a valid command-line flag.");
            printHelpAndExit(errorMessage.toString(),options());
        }

        final String outputDirStr = cmd.getOptionValue("o");

        final File outputDir;

        if (outputDirStr == null) {
            final var date = Calendar.getInstance().getTime();
            final var dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
            final var strCurrentTime = dateFormat.format(date);
            outputDir = new File("output_" + strCurrentTime);
            outputDir.mkdirs();
        } else {
            outputDir = new File(outputDirStr);
            outputDir.mkdirs();
            if (!outputDir.exists()) {
                printHelpAndExit(format("output directory %s could not be created.", outputDir.getAbsolutePath()),
                        configOptions);
            }
        }

        logger.info("input directory: {}", inputDir.getAbsolutePath());
        logger.info("output directory: {}", outputDir.getAbsolutePath());

        final var config = new Config();
        config.setSourceVersion(FedoraVersion.fromString(cmd.getOptionValue("s")));
        config.setTargetVersion(FedoraVersion.fromString(cmd.getOptionValue("t")));
        config.setInputDir(inputDir);
        config.setOutputDir(outputDir);

        if (cmd.hasOption("source-rdf")) {
            config.setSrcRdfLang(RDFLanguages.contentTypeToLang(cmd.getOptionValue("source-rdf")));
        }

        if (cmd.hasOption("threads")) {
            config.setThreads(Integer.valueOf(cmd.getOptionValue("threads")));
        }

        if (config.getTargetVersion().equals(FedoraVersion.V_6)) {
            //base URI only used when migrating to F6
            config.setBaseUri(cmd.getOptionValue("base-uri"));
        }
        if (cmd.hasOption("digest-algorithm")) {
            final var algo = cmd.getOptionValue("digest-algorithm");
            if (!VALID_DIGEST_ALGORITHMS.contains(algo)) {
                printHelpAndExit(format("invalid digest algorithm (%s) provided, must be one of %s",
                        algo, String.join(", ", VALID_DIGEST_ALGORITHMS)), configOptions);
            }
            config.setDigestAlgorithm(cmd.getOptionValue("digest-algorithm"));
        }
        config.setFedoraUser(cmd.getOptionValue("migration-user"));
        config.setFedoraUserAddress(cmd.getOptionValue("migration-user-address"));

        config.setSkipAcls(cmd.hasOption("skip-acls"));

        config.setWriteToS3(cmd.hasOption("write-to-s3"));
        config.setS3Region(cmd.getOptionValue("s3-region"));
        config.setS3Endpoint(cmd.getOptionValue("s3-endpoint"));
        config.setS3PathStyleAccess(cmd.hasOption("s3-path-style-access"));
        config.setS3AccessKey(cmd.getOptionValue("s3-access-key"));
        config.setS3SecretKey(cmd.getOptionValue("s3-secret-key"));
        config.setS3Bucket(cmd.getOptionValue("s3-bucket"));
        config.setS3Prefix(cmd.getOptionValue("s3-prefix"));

        if (cmd.getOptionValue('R') != null) {
            config.setResourceInfoFile(Paths.get(cmd.getOptionValue('R')));
        }

        if (config.getTargetVersion() == FedoraVersion.V_6) {
            if (config.getBaseUri() == null) {
                printHelpAndExit("base-uri must be specified when migrating to Fedora 6", configOptions);
            }
        }

        if (config.isWriteToS3() && StringUtils.isBlank(config.getS3Bucket())) {
            printHelpAndExit("s3-bucket must be specified when writing to S3", configOptions);
        }

        return config;
    }

    private Options options() {
        // Help option
        final Options configOptions = new org.apache.commons.cli.Options();

        configOptions.addOption(Option.builder("h")
                .longOpt("help")
                .hasArg(false)
                .desc("Print these options")
                .required(false)
                .build());

        configOptions.addOption(Option.builder("i")
                .longOpt("input-dir")
                .hasArg(true)
                .desc("The path to the directory containing a Fedora 4.7.x or Fedora 5.x export")
                .required(true)
                .build());

        configOptions.addOption(Option.builder("o")
                .longOpt("output-dir")
                .hasArg(true)
                .desc("The path to the directory where upgraded resources will be written. " +
                        "Default value: output_<yyyyMMdd-HHmmss>. " +
                        "For example: output_20200101-075901")
                .required(false)
                .build());

        configOptions.addOption(Option.builder("s")
                .longOpt("source-version")
                .hasArg(true)
                .desc(format(
                        "The version of Fedora that was the source of the export. Valid values: %s",
                        join(VALID_MIGRATION_PATHS.keySet())))
                .required(true)
                .build());

        configOptions.addOption(Option.builder("t")
                .longOpt("target-version")
                .hasArg(true)
                .desc(format("The version of Fedora to which you are upgrading. Valid values: %s",
                        join(VALID_MIGRATION_PATHS.values().stream()
                                .flatMap(x -> x.stream()).collect(Collectors.toSet()))))
                .required(true)
                .build());

        // F6 migration opts
        configOptions.addOption(Option.builder("r")
                .longOpt("source-rdf")
                .hasArg(true)
                .desc("The RDF language used in the Fedora export. Default: " + Config.DEFAULT_SRC_RDF_LANG.getName())
                .required(false)
                .build());

        configOptions.addOption(Option.builder("u")
                .longOpt("base-uri")
                .hasArg(true)
                .desc("Fedora's base URI. For example, http://localhost:8080/rest")
                .required(false)
                .build());

        configOptions.addOption(Option.builder("p")
                .longOpt("threads")
                .hasArg(true)
                .desc("The number of threads to use. Default: the number of available cores")
                .required(false)
                .build());

        configOptions.addOption(Option.builder("d")
                .longOpt("digest-algorithm")
                .hasArg(true)
                .desc("The digest algorithm to use in OCFL. Default: " + Config.DEFAULT_DIGEST_ALGORITHM)
                .required(false)
                .build());

        configOptions.addOption(Option.builder()
                .longOpt("migration-user")
                .hasArg(true)
                .desc("The user to attribute OCFL versions to. Default: " + Config.DEFAULT_USER)
                .required(false)
                .build());

        configOptions.addOption(Option.builder()
                .longOpt("migration-user-address")
                .hasArg(true)
                .desc("The address of the user OCFL versions are attributed to. Default: "
                        + Config.DEFAULT_USER_ADDRESS)
                .required(false)
                .build());
        configOptions.addOption(Option.builder()
                .longOpt("skip-acls")
                .hasArg(false)
                .desc("Skip creating fcr:acl resources and migrate webac:Acl and acl:Authorization resources normally")
                .required(false)
                .build());

        // S3 options

        configOptions.addOption(Option.builder()
                .longOpt("write-to-s3")
                .hasArg(false)
                .desc("Enables writing migrated Fedora 6 data to S3 rather than the local filesystem")
                .required(false)
                .build());

        configOptions.addOption(Option.builder()
                .longOpt("s3-bucket")
                .hasArg(true)
                .desc("The S3 bucket to write to, required when writing to S3")
                .required(false)
                .build());

        configOptions.addOption(Option.builder()
                .longOpt("s3-prefix")
                .hasArg(true)
                .desc("The S3 prefix to locate the OCFL repo in, optionally use when writing to S3")
                .required(false)
                .build());

        configOptions.addOption(Option.builder()
                .longOpt("s3-region")
                .hasArg(true)
                .desc("The AWS region, optionally use when writing to S3")
                .required(false)
                .build());

        configOptions.addOption(Option.builder()
                .longOpt("s3-endpoint")
                .hasArg(true)
                .desc("The AWS endpoint URL, optionally use when writing to S3")
                .required(false)
                .build());

        configOptions.addOption(Option.builder()
                .longOpt("s3-path-style-access")
                .hasArg(false)
                .desc("The S3 access style, optionally use when writing to S3")
                .required(false)
                .build());

        configOptions.addOption(Option.builder()
                .longOpt("s3-access-key")
                .hasArg(true)
                .desc("The AWS access key, optionally use when writing to S3")
                .required(false)
                .build());

        configOptions.addOption(Option.builder()
                .longOpt("s3-secret-key")
                .hasArg(true)
                .desc("The AWS secret key, optionally use when writing to S3")
                .required(false)
                .build());

        configOptions.addOption(Option.builder("R")
                .longOpt("resource-info-file")
                .hasArg(true)
                .desc("The path the file that contains a list of resources to be processed")
                .required(false)
                .build());

        return configOptions;
    }

    private Object join(final Collection<FedoraVersion> versions) {
        return versions.stream().map(FedoraVersion::getStringValue).collect(Collectors.joining(","));
    }

    private static void printHelpAndExit(final String errorMessage, final Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        System.err.println(errorMessage);
        formatter.printHelp("java -jar fcrepo-upgrade-util-<version>.jar", options);
        System.exit(1);
    }

}
