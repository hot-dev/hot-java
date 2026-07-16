package dev.hot.sdk;

/** A build archive to upload with {@link BuildsResource#upload}. */
public final class BuildUpload {

  private final byte[] content;
  private final String hash;
  private String filename = "build";
  private String buildId;

  /**
   * @param content the build archive content
   * @param hash the build content hash
   */
  public BuildUpload(byte[] content, String hash) {
    this.content = content;
    this.hash = hash;
  }

  /** Names the multipart file part (default "build"). */
  public BuildUpload filename(String filename) {
    this.filename = filename;
    return this;
  }

  /** Optionally pins the build id. */
  public BuildUpload buildId(String buildId) {
    this.buildId = buildId;
    return this;
  }

  public byte[] content() {
    return content;
  }

  public String hash() {
    return hash;
  }

  public String filename() {
    return filename;
  }

  public String buildId() {
    return buildId;
  }
}
