package selo.to.aws.lambda;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class ApplyImageScalerToBucket {
  public static void main(String[] args) throws ParseException, IOException {
    CommandLine commandLine = parseCommandLine(args);
    String bucket = commandLine.getOptionValue("bucket");

    AmazonS3 s3Client = new AmazonS3Client();
    ObjectListing objectListing = s3Client.listObjects(bucket);
    List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();

    while (objectListing.isTruncated()) {
      objectListing = s3Client.listObjects(bucket);
      summaries.addAll(objectListing.getObjectSummaries());
    }

    for (S3ObjectSummary summary : summaries) {
      AwsLambdaS3ImageScaler.scaleImage(summary.getBucketName(), summary.getKey());
    }
  }

  private static CommandLine parseCommandLine(String[] args) throws ParseException {
    CommandLineParser parser = new DefaultParser();
    Options options = new Options();
    options.addOption(new Option("b", "bucket", true, "bucket to apply image scaler to"));
    CommandLine commandLine = parser.parse(options, args);
    return commandLine;
  }
}
