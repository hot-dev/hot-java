package dev.hot.sdk;

/** A build archive to upload with {@link BuildsResource#upload}. */
public final class BuildUpload {

  private final byte[] content;
  private final String hash;
  private String filename = "build";
  private String buildId;

  /**
   * @param content the build archive content (non-null)
   * @param hash the build content hash (non-null, non-empty)
   */
  public BuildUpload(byte[] content, String hash) {
    if (content == null) {
      throw new IllegalArgumentException("content is required");
    }
    if (hash == null || hash.isEmpty()) {
      throw new IllegalArgumentException("hash is required");
    }
    this.content = content;
    this.hash = hash;
  }

  /** Names the multipart file part (default "build"; must be non-null and non-empty). */
  public BuildUpload filename(String filename) {
    if (filename == null || filename.isEmpty()) {
      throw new IllegalArgumentException("filename is required");
    }
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
