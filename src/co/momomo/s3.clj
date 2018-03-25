(ns co.momomo.s3
  (require [clojure.java.io :as io]
           [clojure.string :as ss])
  (import [co.momomo S3UploadOutputStream]
          [java.io InputStream OutputStream ByteArrayInputStream]
          [com.amazonaws.services.s3 AmazonS3Client]
          [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest
            ObjectMetadata ObjectListing S3ObjectSummary]
          [com.amazonaws.auth
            BasicAWSCredentials AWSStaticCredentialsProvider]))
(set! *warn-on-reflection* true)

(defn ^AWSStaticCredentialsProvider creds
  [^String access-key ^String secret]
  (AWSStaticCredentialsProvider. (BasicAWSCredentials. access-key secret)))

(def default-creds
  (delay
    (with-open [ins (io/reader (io/resource "aws_creds"))]
      (let [lines (line-seq ins)]
        (creds (first lines) (second lines))))))

(def s3-client
  (delay (AmazonS3Client. ^AWSStaticCredentialsProvider @default-creds)))



(defn ^InputStream input-stream
  [^String bucket ^String k]
  (->
    ^AmazonS3Client @s3-client
    (.getObject (GetObjectRequest. bucket k))
    (.getObjectContent)))

(defn ^OutputStream output-stream
  [^String bucket ^String k]
  (S3UploadOutputStream/create ^AmazonS3Client @s3-client bucket k))

(defn upload!
  ([^String bucket ^String k ^InputStream ins data-size]
    (let [m (ObjectMetadata.)]
      (when-not (nil? data-size)
        (.setContentLength m data-size))
      (.putObject
        ^AmazonS3Client ^AmazonS3Client @s3-client
        (PutObjectRequest. bucket k ins m))))
  ([^String bucket ^String k ^bytes data]
    (upload! bucket k (ByteArrayInputStream. data) (alength data))))

(defn upload-file!
  [^String bucket ^String k ^java.io.File fd]
  (.putObject ^AmazonS3Client @s3-client (PutObjectRequest. bucket k fd)))

(defn stream-http-response!
  [bucket k response]
  (let [content-length (get (:headers response) "Content-Length")
        content-length (if (nil? content-length) nil (Integer/parseInt content-length))]
    (with-open [ins (io/input-stream (:body response))]
      (upload! bucket k ins content-length))))

(defn- inner-list-bucket
  [^AmazonS3Client s3 ^ObjectListing listing]
  (if (.isTruncated listing)
    (concat
      (.getObjectSummaries (.listNextBatchOfObjects s3 listing))
      (lazy-seq (inner-list-bucket s3 listing)))
    (list)))

(defn list-bucket-summaries
  ([bucket prefix]
    (let [^AmazonS3Client s3 @s3-client
          ^ObjectListing listing (.listObjects s3 bucket prefix)
          ^java.util.List summaries (.getObjectSummaries listing)]
      (loop [listing listing]
        (when (.isTruncated listing)
          (let [listing (.listNextBatchOfObjects s3 listing)]
            (.addAll summaries (.getObjectSummaries listing))
            (recur listing))))
      summaries))
  ([bucket] (list-bucket-summaries bucket "")))


(defn list-bucket
  [bucket]
  (map (fn [^S3ObjectSummary row] (.getKey row))
    (list-bucket-summaries bucket)))
