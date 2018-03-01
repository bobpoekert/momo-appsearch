package co.momomo;

import com.github.yeriomin.playstoreapi.HttpClientAdapter;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.NameValuePair;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.entity.UrlEncodedFormEntity;

import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.net.URISyntaxException;
import java.io.UnsupportedEncodingException;

class ResponseException extends IOException {

    Map<String,String> headers;
    byte[] body;

    public ResponseException(HttpResponse response) {
        this.headers = new HashMap<String,String>();
        HeaderIterator it = response.headerIterator();
        while(it.hasNext()) {
            Header header = it.nextHeader();
            this.headers.put(header.getName(), header.getValue());
        }

        try {
            this.body = ByteStreams.toByteArray(response.getEntity().getContent());
        } catch (IOException e) {
            this.body = null;
        }
    }

    public Map<String,String> getHeaders() {
        return this.headers;
    }

    public byte[] getBody() {
        return this.body;
    }


}

public class ApacheHttpClientAdapter extends HttpClientAdapter {


    public ApacheHttpClientAdapter() {
    }

    public CloseableHttpClient getClient() {
        return HttpClients.createDefault();
    }

    public String buildUrl(String url, Map<String,String> params) {
        try {
            URIBuilder uriBuilder = new URIBuilder(url);
            for (Map.Entry<String,String> row : params.entrySet()) {
                uriBuilder = uriBuilder.setParameter(row.getKey(), row.getValue());
            }
            return uriBuilder.build().toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public String buildUrlEx(String url, Map<String, List<String>> params) {
        try {
            URIBuilder uriBuilder = new URIBuilder(url);
            for (Map.Entry<String, List<String>> row : params.entrySet()) {
                String key = row.getKey();
                for (String val : row.getValue()) {
                    uriBuilder = uriBuilder.setParameter(key, val);
                }
            }
            return uriBuilder.build().toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    byte[] handleRequest(HttpUriRequest req) throws IOException {
        try (CloseableHttpClient client = getClient()) {
            HttpResponse response = client.execute(req);

            if (response.getStatusLine().getStatusCode() == 200) {
                return ByteStreams.toByteArray(response.getEntity().getContent());
            } else {
                throw new ResponseException(response);
            }
        }
    }

    void setHeaders(HttpUriRequest req, Map<String,String> headers) {
        for (Map.Entry<String,String> row : headers.entrySet()) {
            req.addHeader(row.getKey(), row.getValue());
        }
    }

    @Override
    public byte[] get(String url, Map<String,String> params, Map<String,String> headers) throws IOException {

        HttpGet req = new HttpGet(buildUrl(url, params));
        setHeaders(req, headers);
        return handleRequest(req);

    }
   
    @Override
    public byte[] getEx(String url, Map<String,List<String>> params, Map<String,String> headers)
        throws IOException {

        HttpGet req = new HttpGet(buildUrlEx(url, params));
        setHeaders(req, headers);
        return handleRequest(req);

    }

    HttpEntity toPostBody(Map<String,String> inp) {
        NameValuePair[] res = new NameValuePair[inp.size()];
        int i = 0;
        for (Map.Entry<String,String> row : inp.entrySet()) {
            res[i++] = new BasicNameValuePair(row.getKey(), row.getValue());
        }
        try {
            return new UrlEncodedFormEntity(Arrays.asList(res), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] post(String url, byte[] body, Map<String,String> headers) throws IOException {
        if (!headers.containsKey("Content-Type")) {
            headers.put("Content-Type", "application/x-protobuf");
        }

        HttpPost req = new HttpPost(url);
        setHeaders(req, headers);
        req.setEntity(new ByteArrayEntity(body));
        return handleRequest(req);
    }

    @Override
    public byte[] post(String url, Map<String,String> params, Map<String,String> headers) throws IOException {
        headers.put("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");

        HttpPost req = new HttpPost(url);
        setHeaders(req, headers);
        req.setEntity(toPostBody(params));
        return handleRequest(req);
    }

    @Override
    public byte[] postWithoutBody(String url, Map<String,String> urlParams, Map<String,String> headers)
        throws IOException {
        HttpPost req = new HttpPost(buildUrl(url, urlParams));
        setHeaders(req, headers);
        return handleRequest(req);
    }

}
