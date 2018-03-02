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
