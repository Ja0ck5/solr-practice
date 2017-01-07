package cn.lyj.solrj.service;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.junit.Test;

import cn.lyj.solrj.pojo.EasyUIResult;
import cn.lyj.solrj.pojo.Item;

public class ItemDataImport {

	@Test
	public void testImport() throws Exception {
		//从数据库查询数据 http://manage.mallmall.com/rest/item?page=1&rows=30
		String url = "http://manage.mallmall.com/rest/item?page={page}&rows=100";
		
		  // 在url中指定core名称：taotao
		//http://solr.mallmall.com/#/mallmall 后台管理网页地址
        String solrUrl = "http://solr.mallmall.com/mallmall";//solr 服务地址
        HttpSolrServer httpSolrServer = new HttpSolrServer(solrUrl); //定义solr的server
        httpSolrServer.setParser(new XMLResponseParser()); // 设置响应解析器
        httpSolrServer.setMaxRetries(1); // 设置重试次数，推荐设置为1
        httpSolrServer.setConnectionTimeout(500); // 建立连接的最长时间

//        SolrjService solrjService = new SolrjService(httpSolrServer);
        
        
		//分页获取数据
		Integer page = 1;
		Integer pageSize = 0;
		
		do {
			String jsonData = doGet(StringUtils.replace(url, "{page}", String.valueOf(page)));
			EasyUIResult easyUIResult = EasyUIResult.formatToList(jsonData, Item.class);
			List<Item> rows = (List<Item>) easyUIResult.getRows();
			pageSize = rows.size();
			
			httpSolrServer.addBeans(rows);
			httpSolrServer.commit();
			page++;
		} while (100 == pageSize);
		//将商品数据写入到solr
	}
	
	private String doGet(String url){


        // 创建Httpclient对象
        CloseableHttpClient httpclient = HttpClients.createDefault();

        // 创建http GET请求
        HttpGet httpGet = new HttpGet(url);

        CloseableHttpResponse response = null;
        try {
			try {
			    // 执行请求
			    response = httpclient.execute(httpGet);
			    // 判断返回状态是否为200
			    if (response.getStatusLine().getStatusCode() == 200) {
			        return  EntityUtils.toString(response.getEntity(), "UTF-8");
			    }
			} finally {
			    if (response != null) {
			        response.close();
			    }
			    httpclient.close();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
