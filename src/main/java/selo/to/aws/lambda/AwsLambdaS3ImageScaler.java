package selo.to.aws.lambda;

import static java.util.Optional.empty;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
  public static final Pattern FILE_TYPE_PATTERN = Pattern.compile(".*\\.([^\\.]*)");
  public static final String PREFIX = "scaled";
  private static final List<Integer> SCALE_DIMENSIONS = new ArrayList<Integer>();

  static {
    SCALE_DIMENSIONS.add(200);
    SCALE_DIMENSIONS.add(400);
    SCALE_DIMENSIONS.add(800);
  }

  public static final String JPG_TYPE = "jpg";
  private static final String JPG_MIME = "image/jpeg";
  public static final String PNG_TYPE = "png";
  private static final String PNG_MIME = "image/png";

  public String handleRequest(S3Event s3event, Context context) {
    try {
      S3EventNotificationRecord record = s3event.getRecords().get(0);

      String bucket = record.getS3().getBucket().getName();
      // Object key may have spaces or unicode non-ASCII characters.
      String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
      srcKey = URLDecoder.decode(srcKey, "UTF-8");

      return scaleImage(bucket, srcKey);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String scaleImage(String bucket, String key) throws IOException {
    if (key.startsWith(PREFIX)) {
      System.out.println("Target image is already scaled");
      return "Nothing";
    }

    Optional<String> optionalImageType = getImageType(key);
    if (!optionalImageType.isPresent()) {
      return "";
    }
    String imageType = optionalImageType.get();

    // Download the image from S3 into a stream
    AmazonS3 s3Client = new AmazonS3Client();
    S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
    InputStream objectData = s3Object.getObjectContent();

    // Read the source image
    BufferedImage srcImage = ImageIO.read(objectData);
    int srcHeight = srcImage.getHeight();
    int srcWidth = srcImage.getWidth();

    for (int scaleDimension : SCALE_DIMENSIONS) {

      String dstKey = PREFIX + "-" + scaleDimension + "-" + key;

      // Infer the scaling factor to avoid stretching the image
      // unnaturally
      float scalingFactor =
          Math.min((float) scaleDimension / srcWidth, (float) scaleDimension / srcHeight);
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
      System.out.println("Writing to: " + bucket + "/" + dstKey);
      s3Client.putObject(bucket, dstKey, is, meta);
      System.out.println("Successfully resized " + bucket + "/" + key + " and uploaded to " + bucket
          + "/" + dstKey);
    }
    return "Ok";
  }

  /**
   * Source: https://today.java.net/pub/a/today/2007/04/03/perils-of-image-getscaledinstance.html
   */
  public static BufferedImage getHighQualityScaledInstance(BufferedImage img, int targetWidth,
      int targetHeight, Object hint) {
    int type = BufferedImage.TYPE_INT_RGB;
    BufferedImage ret = deepCopy(img);
    int w, h;
    // Use multi-step technique: start with original size, then
    // scale down in multiple passes with drawImage()
    // until the target size is reached
    w = img.getWidth();
    h = img.getHeight();

    do {
      if (w > targetWidth) {
        w /= 2;
      }
      if (w < targetWidth) {
        w = targetWidth;
      }
      if (h > targetHeight) {
        h /= 2;
      }
      if (h < targetHeight) {
        h = targetHeight;
      }

      BufferedImage tmp = new BufferedImage(w, h, type);
      Graphics2D g2 = tmp.createGraphics();
      // Fill with white before applying semi-transparent (alpha) images
      g2.setPaint(Color.white);
      g2.fillRect(0, 0, w, h);

      g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, hint);
      g2.drawImage(ret, 0, 0, w, h, null);
      g2.dispose();

      ret = tmp;
    } while (w != targetWidth || h != targetHeight);

    return ret;
  }

  static BufferedImage deepCopy(BufferedImage bi) {
    ColorModel cm = bi.getColorModel();
    boolean isAlphaPremultiplied = cm.isAlphaPremultiplied();
    WritableRaster raster = bi.copyData(null);
    return new BufferedImage(cm, raster, isAlphaPremultiplied, null);
  }

  private static Optional<String> getImageType(String key) {
    Matcher matcher = FILE_TYPE_PATTERN.matcher(key);
    if (!matcher.matches()) {
      System.out.println("Unable to infer image type for key " + key);
      return empty();
    }
    String imageType = matcher.group(1);
    if (!(JPG_TYPE.equals(imageType)) && !(PNG_TYPE.equals(imageType))) {
      System.out.println("Skipping non-image " + key);
      return empty();
    }

    return Optional.of(imageType);
  }

}
