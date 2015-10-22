package selo.to.aws.lambda;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

public class AwsLambdaS3ImageScaler implements RequestHandler<S3Event, String> {
  private static final String PREFIX = "thumbnail-";
  private static final float MAX_WIDTH = 200;
  private static final float MAX_HEIGHT = 200;
  private final String JPG_TYPE = "jpg";
  private final String JPG_MIME = "image/jpeg";
  private final String PNG_TYPE = "png";
  private final String PNG_MIME = "image/png";

  public String handleRequest(S3Event s3event, Context context) {
    try {
      S3EventNotificationRecord record = s3event.getRecords().get(0);

      String srcBucket = record.getS3().getBucket().getName();
      // Object key may have spaces or unicode non-ASCII characters.
      String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
      srcKey = URLDecoder.decode(srcKey, "UTF-8");

      if (srcKey.startsWith(PREFIX)) {
        System.out.println("Target image is already a thumbnail");
        return "Nothing";
      }

      String dstBucket = srcBucket;
      String dstKey = PREFIX + srcKey;

      // Infer the image type.
      Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey);
      if (!matcher.matches()) {
        System.out.println("Unable to infer image type for key " + srcKey);
        return "";
      }
      String imageType = matcher.group(1);
      if (!(JPG_TYPE.equals(imageType)) && !(PNG_TYPE.equals(imageType))) {
        System.out.println("Skipping non-image " + srcKey);
        return "";
      }

      // Download the image from S3 into a stream
      AmazonS3 s3Client = new AmazonS3Client();
      S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
      InputStream objectData = s3Object.getObjectContent();

      // Read the source image
      BufferedImage srcImage = ImageIO.read(objectData);
      int srcHeight = srcImage.getHeight();
      int srcWidth = srcImage.getWidth();
      // Infer the scaling factor to avoid stretching the image
      // unnaturally
      float scalingFactor = Math.min(MAX_WIDTH / srcWidth, MAX_HEIGHT / srcHeight);
      int width = (int) (scalingFactor * srcWidth);
      int height = (int) (scalingFactor * srcHeight);

      BufferedImage resizedImage = getHighQualityScaledInstance(srcImage, width, height,
          RenderingHints.VALUE_INTERPOLATION_BICUBIC);

      // Re-encode image to target format
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      ImageIO.write(resizedImage, imageType, os);
      InputStream is = new ByteArrayInputStream(os.toByteArray());
      // Set Content-Length and Content-Type
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentLength(os.size());
      if (JPG_TYPE.equals(imageType)) {
        meta.setContentType(JPG_MIME);
      }
      if (PNG_TYPE.equals(imageType)) {
        meta.setContentType(PNG_MIME);
      }

      // Uploading to S3 destination bucket
      System.out.println("Writing to: " + dstBucket + "/" + dstKey);
      s3Client.putObject(dstBucket, dstKey, is, meta);
      System.out.println("Successfully resized " + srcBucket + "/" + srcKey + " and uploaded to "
          + dstBucket + "/" + dstKey);
      return "Ok";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Source: https://today.java.net/pub/a/today/2007/04/03/perils-of-image-getscaledinstance.html
   */
  public BufferedImage getHighQualityScaledInstance(BufferedImage img, int targetWidth,
      int targetHeight, Object hint) {
    int type = BufferedImage.TYPE_INT_RGB;
    BufferedImage ret = (BufferedImage) img;
    int w, h;
    // Use multi-step technique: start with original size, then
    // scale down in multiple passes with drawImage()
    // until the target size is reached
    w = img.getWidth();
    h = img.getHeight();

    do {
      if (w > targetWidth) {
        w /= 2;
        if (w < targetWidth) {
          w = targetWidth;
        }
      }

      if (h > targetHeight) {
        h /= 2;
        if (h < targetHeight) {
          h = targetHeight;
        }
      }

      BufferedImage tmp = new BufferedImage(w, h, type);
      Graphics2D g2 = tmp.createGraphics();
      g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, hint);
      g2.drawImage(ret, 0, 0, w, h, null);
      g2.dispose();

      ret = tmp;
    } while (w != targetWidth || h != targetHeight);

    return ret;
  }

}
