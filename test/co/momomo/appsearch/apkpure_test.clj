(ns co.momomo.appsearch.apkpure_test
  (import [org.jsoup Jsoup])
  (require [co.momomo.appsearch.apkpure :refer :all]
           [clojure.test :refer :all]))

(deftest test-app-meta
  (testing "keepvid"
    (is
      (=
        (app-meta (Jsoup/parse (java.io.File. "./test-data/keepvid.html") "UTF-8"))
        '{:description
 "KeepVid Android is a free video downloader app, and it allows you to download videos from YouTube, Facebook and other 28 video sharing sites. KeepVid Video Downloader empowers a very clean user interface, and easy-to-handle downloading engine. With the built-in searching feature, you can search for the video you want to download directly. Moreover, KeepVid Android allows you to download HD videos with ease, including 1080p, 4k and more. If you want to download multiple videos at the same time, you are able to get the work done with the help of KeepVid Android Video Downloader. 100% Free Video Downloader App for Android. KeepVid Android doesn’t have any in-app purchase, so you can take full control of this app easily. Download HD Videos without Quality Loss. KeepVid Android allows you to download HD videos with no quality loss. If the videos are available in 1080p, 2k or 4k, you can save the videos in their original qualitis. Download YouTube to MP3 Directly. KeepVid Android enables you to download YouTube to MP3 audio files directly without converting the video after downloading. Download Multiple Videos in a BatchThis powerful video downloader app enables you to download multiple videos at the same time to improve the downloading using experience. Search YouTube and Download Directly. This YouTube downloader app allows you to search for the videos you want to download directly with the built-in searching feature, and then download the video directly. Fast Downloading SpeedWith the built-in turbo downloading engine, KeepVid Android offers a super-fast downloading speed for you to download the videos from video sharing sites. Low Battery Consumption for Android Devices. KeepVid Android is equipped with the new technology which helps you to save battery life on your Android device when it’s working.\"",
 :author_url "/developer/KeepVid%20Studio",
 :appstore_links (),
 :thumbnail_url
 "https://image.winudf.com/v2/image/YWRtaW5fNTEyLnBuZ18xNTAxNDgyMjUzMzIy/icon.png?w=170&fakeurl=1&type=.png",
 :title "KeepVid",
 :category_url "/tools",
 :snapshot_image_urls_800
 ("https://image.winudf.com/v2/image/YWRtaW5fUzYxMjI4LTA5NDM0MS5qcGdfMTUwMTQ4MjU2MDQyMA/screen-0.jpg?h=800&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzYxMjI4LTA5NDM1Mi5qcGdfMTUwMTQ4MjU1MjYxMw/screen-1.jpg?h=800&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzYxMjI4LTA5NDk1Mi5qcGdfMTUwMTQ4MjU1NzcwNg/screen-2.jpg?h=800&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwMTA1LTE0MjI0OS5qcGdfMTUwMTQ4MjU2MzI2Nw/screen-3.jpg?h=800&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwMTA1LTE0MjcwNi5qcGdfMTUwMTQ4MjU0OTgwMQ/screen-4.jpg?h=800&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwMTE3LTE1NDUxOC5qcGdfMTUwMTQ4MjU0MTY3MA/screen-5.jpg?h=800&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwNjI4LTEzNDg0Ny5qcGdfMTUwMTQ4MjU1ODE4NA/screen-6.jpg?h=800&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwNzI3LTE2Mjc0Ni5qcGdfMTUwMTQ4MjU2MzI0Nw/screen-7.jpg?h=800&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwNzI3LTE2MjgwMy5qcGdfMTUwMTQ4MjU0Nzk2NQ/screen-8.jpg?h=800&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwNzI3LTE2MzMyOC5qcGdfMTUwMTQ4MjU0NjgwMQ/screen-9.jpg?h=800&fakeurl=1&type=.jpg"),
 :author_name "KeepVid Studio",
 :versions
 ({:url
   "/keepvid-android/com.keepvid.studio/download/24-APK?from=versions%2Fversion",
   :updated_on #inst "2017-12-11T22:06:55.200-00:00",
   :android_version "Android 4.0.3+ (Ice Cream Sandwich MR1, API 15)",
   :signature "ea1dad3415872138efcc56d628c2cd909b72e5ff",
   :file_sha1 "50838328b6ba781461d1e8337a445e9c75dc766e",
   :file_size "11.3 MB"}
  {:url
   "/keepvid-android/com.keepvid.studio/download/18-APK?from=versions%2Fversion",
   :updated_on #inst "2017-07-31T21:06:55.210-00:00",
   :android_version "Android 4.0.3+ (Ice Cream Sandwich MR1, API 15)",
   :signature "ea1dad3415872138efcc56d628c2cd909b72e5ff",
   :file_sha1 "b9fb95375d7addc1e9fc794b0b2beeca6ece89a2",
   :file_size "8.0 MB"}),
 :screenshot_image_urls_350
 ("https://image.winudf.com/v2/image/YWRtaW5fUzYxMjI4LTA5NDM0MS5qcGdfMTUwMTQ4MjU2MDQyMA/screen-0.jpg?h=355&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzYxMjI4LTA5NDM1Mi5qcGdfMTUwMTQ4MjU1MjYxMw/screen-1.jpg?h=355&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzYxMjI4LTA5NDk1Mi5qcGdfMTUwMTQ4MjU1NzcwNg/screen-2.jpg?h=355&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwMTA1LTE0MjI0OS5qcGdfMTUwMTQ4MjU2MzI2Nw/screen-3.jpg?h=355&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwMTA1LTE0MjcwNi5qcGdfMTUwMTQ4MjU0OTgwMQ/screen-4.jpg?h=355&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwMTE3LTE1NDUxOC5qcGdfMTUwMTQ4MjU0MTY3MA/screen-5.jpg?h=355&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwNjI4LTEzNDg0Ny5qcGdfMTUwMTQ4MjU1ODE4NA/screen-6.jpg?h=355&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwNzI3LTE2Mjc0Ni5qcGdfMTUwMTQ4MjU2MzI0Nw/screen-7.jpg?h=355&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwNzI3LTE2MjgwMy5qcGdfMTUwMTQ4MjU0Nzk2NQ/screen-8.jpg?h=355&fakeurl=1&type=.jpg"
  "https://image.winudf.com/v2/image/YWRtaW5fUzcwNzI3LTE2MzMyOC5qcGdfMTUwMTQ4MjU0NjgwMQ/screen-9.jpg?h=355&fakeurl=1&type=.jpg"),
 :category_tags ("Free" "Tools APP")}))))
          
