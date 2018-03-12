(ns co.momomo.s3
  (require [clojure.java.io :as io]
           [clojure.string :as ss])
  (import [co.momomo S3UploadOutputStream]
          [java.io InputStream OutputStream]
          [com.amazonaws.services.s3 AmazonS3Client]
          [com.amazonaws.services.s3.model GetObjectRequest]
          [com.amazonaws.auth
            BasicAWSCredentials AWSStaticCredentialsProvider]))

(defn ^AWSStaticCredentialsProvider creds
  [^String access-key ^String secret]
  (AWSStaticCredentialsProvider. (BasicAWSCredentials. access-key secret)))

(def default-creds
  (delay
    (with-open [ins (io/reader (io/resource "aws_creds"))]
      (let [lines (line-seq ins)]
        (creds (first lines) (second lines))))))

(def s3-client
  (delay (AmazonS3Client. @default-creds)))

(defn ^InputStream input-stream
  [^String bucket ^String k]
  (->
    @s3-client
    (.getObject (GetObjectRequest. bucket k))
    (.getObjectContent)))

(defn ^OutputStream output-stream
  [^String bucket ^String k]
  (S3UploadOutputStream/create @s3-client bucket k))
