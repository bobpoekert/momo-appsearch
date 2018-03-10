(ns co.momomo.appsearch.apkpure
  (require [clojure.string :as ss]
           [clojure.java.io :as io]
           [co.momomo.soup :refer :all]
           [co.momomo.appsearch.html-reader :refer [xz-pages-from-url]]
           [co.momomo.cereal :as cereal]
           [cheshire.core :as json])
  (import [org.jsoup Jsoup]
          [org.jsoup.nodes Element]
          [java.io InputStream OutputStream]
          [org.tukaani.xz XZOutputStream LZMA2Options]
          [com.joestelmach.natty Parser DateGroup]))

(set! *warn-on-reflection* true)

(defn parse-date
  [^String d]
  (for [^DateGroup g (.parse (Parser.) d)
        date (.getDates g)]
    date))

(defn parse-infobox-row
  [^Element row]
  (let [k (select-strip row (tag "strong"))
        v (-> row (.parent) (.text))]
    {k v}))

(defn parse-app-version-meta
  [^String k ^String v]
  (let [k (.toLowerCase (.trim k))]
    (case k
      "signature:" {:signature (.trim v)}
      "file sha1:" {:file_sha1 (.trim v)}
      "update on:" {:updated_on (first (parse-date v))}
      "requires android:" {:android_version (.trim v)}
      "file size:" {:file_size (.trim v)}
      {k v})))

(defn extract-app-versions
  [tree]
  (for [version (select tree (path (%and (tag "ul") (has-class "ver-wrap")) (tag "li")))]
    (apply merge
      {:url (first (select-attr version "href" (tag "a")))}
      (for [^Element row (select version (path (has-class "ver-info-m") (tag "p")))]
        (if (empty? (.getElementsByTag row "a"))
          (parse-app-version-meta
            (.text ^Element (first (.getElementsByTag row "strong")))
            (.trim (.ownText row)))
          {})))))

(def category-a 
  (->
    (any-pos (has-class "additional"))
    (parent)
    (parent)
    (any-path (%and (tag "a") (kv-val-contains "title" "Download more")))))

(def thumbnail-selector 
  (any-path
    (path
      (%and (tag "div") (has-class "describe"))
      (%and (tag "div") (has-class "describe-img")))
    (%and (tag "a") (kv "target" "_blank"))))

(defn app-meta
  [tree]
  (let [infobox (select tree (path (id "fax_box_faq2") (tag "dl") (tag "dd") (tag "p")))
        infobox (if infobox (first infobox))
        title-box (first (select tree 
                          (any-pos
                            (path
                              (%and (tag "div") (has-class "box"))
                              (%and (tag "dl") (has-class "ny-dl"))))))]
      {:thumbnail_url (first (select-attr tree "src"
                        (path
                          (%and (tag "div") (has-class "icon"))
                          (tag "img"))))
      :title (select-strip title-box
              (path
                (%and (tag "div") (has-class "title-like"))
                (tag "h1")))
      :author_name (select-strip tree
                    (any-path
                      (kv "itemtype" "http://schema.org/Organization")
                      (tag "span")))
      :author_url (first
                    (select-attr tree "href"
                      (path
                        (kv "itemtype" "http://schema.org/Organization")
                        (tag "a"))))
      :download_url (first
                      (select-attr tree "href"
                        (any-pos
                          (path
                            (%and (tag "div") (has-class "ny-down"))
                            (%and (tag "a") (has-class "da"))))))
      :description (try
                    (->>
                      (select tree
                        (path
                          (id "describe")
                          (tag "div")
                          (tag "div")))
                      (map get-text)
                      (ss/join "\n"))
                    (catch Exception e
                      (select-strip tree
                        (any-pos
                          (path
                            (id "describe")
                            (%and (tag "div") (has-class "description"))
                            (%and (tag "div") (has-class "content")))))))
      :category_url (first (select-attr tree "href" category-a))
      :category_tags (map get-text (select tree (path category-a (tag "span"))))
      :appstore_links (->>
                        (select tree
                          (any-pos
                            (%and (tag "a") (kv-val-contains "ga" "get_it_on"))))
                        (map #(getattr % "href")))
      :versions (extract-app-versions tree)
      :snapshot_image_urls_800 (select-attr tree "href" thumbnail-selector)
      :screenshot_image_urls_350 (select-attr tree "src" (path thumbnail-selector (tag "img")))}))

(def urls ["xaa.html.xz","xab.html.xz","xac.html.xz","xad.html.xz","xae.html.xz","xaf.html.xz","xag.html.xz","xah.html.xz","xai.html.xz","xaj.html.xz","xak.html.xz","xal.html.xz","xam.html.xz","xan.html.xz","xao.html.xz","xap.html.xz","xaq.html.xz","xar.html.xz","xas.html.xz","xat.html.xz","xau.html.xz","xav.html.xz","xaw.html.xz","xax.html.xz","xay.html.xz","xaz.html.xz","xba.html.xz","xbb.html.xz","xbc.html.xz","xbd.html.xz","xbe.html.xz","xbf.html.xz","xbg.html.xz","xbh.html.xz","xbi.html.xz","xbj.html.xz","xbk.html.xz","xbl.html.xz","xbm.html.xz","xbn.html.xz","xbo.html.xz","xbp.html.xz","xbq.html.xz","xbr.html.xz","xbs.html.xz","xbt.html.xz","xbu.html.xz","xbv.html.xz","xbw.html.xz","xbx.html.xz","xby.html.xz","xbz.html.xz","xca.html.xz","xcb.html.xz","xcc.html.xz","xcd.html.xz","xce.html.xz","xcf.html.xz","xcg.html.xz","xch.html.xz","xci.html.xz","xcj.html.xz","xck.html.xz","xcl.html.xz","xcm.html.xz","xcn.html.xz","xco.html.xz","xcp.html.xz","xcq.html.xz","xcr.html.xz","xcs.html.xz","xct.html.xz","xcu.html.xz","xcv.html.xz","xcw.html.xz","xcx.html.xz","xcy.html.xz","xcz.html.xz","xda.html.xz","xdb.html.xz","xdc.html.xz","xdd.html.xz","xde.html.xz","xdf.html.xz","xdg.html.xz","xdh.html.xz","xdi.html.xz","xdj.html.xz","xdk.html.xz","xdl.html.xz","xdm.html.xz","xdn.html.xz","xdo.html.xz","xdp.html.xz","xdq.html.xz","xdr.html.xz","xds.html.xz","xdt.html.xz","xdu.html.xz","xdv.html.xz","xdw.html.xz","xdx.html.xz","xdy.html.xz","xdz.html.xz","xea.html.xz","xeb.html.xz","xec.html.xz","xed.html.xz","xee.html.xz","xef.html.xz","xeg.html.xz","xeh.html.xz","xei.html.xz","xej.html.xz","xek.html.xz","xel.html.xz","xem.html.xz","xen.html.xz","xeo.html.xz","xep.html.xz","xeq.html.xz","xer.html.xz","xes.html.xz","xet.html.xz","xeu.html.xz","xev.html.xz","xew.html.xz","xex.html.xz","xey.html.xz","xez.html.xz","xfa.html.xz","xfb.html.xz","xfc.html.xz","xfd.html.xz","xfe.html.xz","xff.html.xz","xfg.html.xz","xfh.html.xz","xfi.html.xz","xfj.html.xz","xfk.html.xz","xfl.html.xz","xfm.html.xz","xfn.html.xz","xfo.html.xz","xfp.html.xz","xfq.html.xz","xfr.html.xz","xfs.html.xz","xft.html.xz","xfu.html.xz","xfv.html.xz","xfw.html.xz","xfx.html.xz","xfy.html.xz","xfz.html.xz","xga.html.xz","xgb.html.xz","xgc.html.xz","xgd.html.xz","xge.html.xz","xgf.html.xz","xgg.html.xz","xgh.html.xz","xgi.html.xz","xgj.html.xz","xgk.html.xz","xgl.html.xz","xgm.html.xz","xgn.html.xz","xgo.html.xz","xgp.html.xz","xgq.html.xz","xgr.html.xz","xgs.html.xz","xgt.html.xz","xgu.html.xz","xgv.html.xz","xgw.html.xz","xgx.html.xz","xgy.html.xz","xgz.html.xz","xha.html.xz","xhb.html.xz","xhc.html.xz","xhd.html.xz","xhe.html.xz","xhf.html.xz","xhg.html.xz","xhh.html.xz","xhi.html.xz","xhj.html.xz","xhk.html.xz","xhl.html.xz","xhm.html.xz","xhn.html.xz","xho.html.xz","xhp.html.xz","xhq.html.xz","xhr.html.xz","xhs.html.xz","xht.html.xz","xhu.html.xz","xhv.html.xz","xhw.html.xz","xhx.html.xz","xhy.html.xz","xhz.html.xz","xia.html.xz","xib.html.xz","xic.html.xz","xid.html.xz","xie.html.xz","xif.html.xz","xig.html.xz","xih.html.xz","xii.html.xz","xij.html.xz","xik.html.xz","xil.html.xz","xim.html.xz","xin.html.xz","xio.html.xz","xip.html.xz","xiq.html.xz","xir.html.xz","xis.html.xz","xit.html.xz","xiu.html.xz","xiv.html.xz","xiw.html.xz","xix.html.xz","xiy.html.xz","xiz.html.xz","xja.html.xz","xjb.html.xz","xjc.html.xz","xjd.html.xz","xje.html.xz","xjf.html.xz","xjg.html.xz","xjh.html.xz","xji.html.xz","xjj.html.xz","xjk.html.xz","xjl.html.xz","xjm.html.xz","xjn.html.xz","xjo.html.xz","xjp.html.xz","xjq.html.xz","xjr.html.xz","xjs.html.xz","xjt.html.xz","xju.html.xz","xjv.html.xz","xjw.html.xz","xjx.html.xz","xjy.html.xz","xjz.html.xz","xka.html.xz","xkb.html.xz","xkc.html.xz","xkd.html.xz","xke.html.xz","xkf.html.xz","xkg.html.xz","xkh.html.xz","xki.html.xz","xkj.html.xz","xkk.html.xz","xkl.html.xz","xkm.html.xz","xkn.html.xz","xko.html.xz","xkp.html.xz","xkq.html.xz","xkr.html.xz","xks.html.xz","xkt.html.xz","xku.html.xz","xkv.html.xz","xkw.html.xz","xkx.html.xz","xky.html.xz","xkz.html.xz","xla.html.xz","xlb.html.xz","xlc.html.xz","xld.html.xz","xle.html.xz","xlf.html.xz","xlg.html.xz","xlh.html.xz","xli.html.xz","xlj.html.xz","xlk.html.xz","xll.html.xz","xlm.html.xz","xln.html.xz","xlo.html.xz","xlp.html.xz","xlq.html.xz","xlr.html.xz","xls.html.xz","xlt.html.xz","xlu.html.xz","xlv.html.xz","xlw.html.xz","xlx.html.xz","xly.html.xz","xlz.html.xz","xma.html.xz","xmb.html.xz","xmc.html.xz","xmd.html.xz","xme.html.xz","xmf.html.xz","xmg.html.xz","xmh.html.xz","xmi.html.xz","xmj.html.xz","xmk.html.xz","xml.html.xz","xmm.html.xz","xmn.html.xz","xmo.html.xz","xmp.html.xz","xmq.html.xz","xmr.html.xz","xms.html.xz","xmt.html.xz","xmu.html.xz","xmv.html.xz","xmw.html.xz","xmx.html.xz","xmy.html.xz","xmz.html.xz","xna.html.xz","xnb.html.xz","xnc.html.xz","xnd.html.xz","xne.html.xz","xnf.html.xz","xng.html.xz","xnh.html.xz","xni.html.xz","xnj.html.xz","xnk.html.xz","xnl.html.xz","xnm.html.xz","xnn.html.xz","xno.html.xz","xnp.html.xz","xnq.html.xz","xnr.html.xz","xns.html.xz","xnt.html.xz","xnu.html.xz","xnv.html.xz","xnw.html.xz","xnx.html.xz","xny.html.xz","xnz.html.xz","xoa.html.xz","xob.html.xz","xoc.html.xz","xod.html.xz","xoe.html.xz","xof.html.xz","xog.html.xz","xoh.html.xz","xoi.html.xz","xoj.html.xz","xok.html.xz","xol.html.xz","xom.html.xz","xon.html.xz","xoo.html.xz","xop.html.xz","xoq.html.xz","xor.html.xz","xos.html.xz","xot.html.xz","xou.html.xz","xov.html.xz","xow.html.xz","xox.html.xz","xoy.html.xz","xoz.html.xz","xpa.html.xz","xpb.html.xz","xpc.html.xz","xpd.html.xz","xpe.html.xz","xpf.html.xz","xpg.html.xz","xph.html.xz","xpi.html.xz","xpj.html.xz","xpk.html.xz","xpl.html.xz","xpm.html.xz","xpn.html.xz","xpo.html.xz","xpp.html.xz","xpq.html.xz","xpr.html.xz","xps.html.xz","xpt.html.xz","xpu.html.xz","xpv.html.xz","xpw.html.xz","xpx.html.xz","xpy.html.xz","xpz.html.xz","xqa.html.xz","xqb.html.xz","xqc.html.xz","xqd.html.xz","xqe.html.xz","xqf.html.xz","xqg.html.xz","xqh.html.xz","xqi.html.xz","xqj.html.xz","xqk.html.xz","xql.html.xz","xqm.html.xz","xqn.html.xz","xqo.html.xz","xqp.html.xz","xqq.html.xz","xqr.html.xz","xqs.html.xz","xqt.html.xz","xqu.html.xz","xqv.html.xz","xqw.html.xz","xqx.html.xz","xqy.html.xz","xqz.html.xz","xra.html.xz","xrb.html.xz","xrc.html.xz","xrd.html.xz","xre.html.xz","xrf.html.xz","xrg.html.xz","xrh.html.xz","xri.html.xz","xrj.html.xz","xrk.html.xz","xrl.html.xz","xrm.html.xz","xrn.html.xz","xro.html.xz","xrp.html.xz","xrq.html.xz","xrr.html.xz","xrs.html.xz","xrt.html.xz","xru.html.xz","xrv.html.xz"])

(defn parsed-pages
  [url]
  (map 
    (fn [^String page] (app-meta (Jsoup/parse page)))
    (xz-pages-from-url (str "http://pi/4tb/apkpure/archive/" url))))

(defn parse-pages!
  [outf]
  (cereal/par-process-into-file!
    (mapcat parsed-pages) urls outf))
