package com.eg.compress;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ZipUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.*;
import com.github.makewheels.s3util.S3Config;
import com.github.makewheels.s3util.S3Service;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CompressHandler {
    private final S3Service s3Service = new S3Service();

    public void initS3() {
        S3Config s3Config = new S3Config();
        s3Config.setEndpoint(System.getenv("s3_endpoint"));
        s3Config.setRegion(System.getenv("s3_region"));
        s3Config.setBucketName(System.getenv("s3_bucketName"));
        s3Config.setAccessKey(System.getenv("s3_accessKey"));
        s3Config.setSecretKey(System.getenv("s3_secretKey"));
        s3Service.init(s3Config);
    }

    /**
     * 读取配置列表
     */
    private JSONObject getConfig() {
        File configFile = new File(CompressHandler.class.getResource("/config.json").getPath());
        String configJson = FileUtil.readUtf8String(configFile);
        return JSONObject.parseObject(configJson);
    }

    private void handleEachPath(JSONObject config, String prefix, int fileAmountForCompress) {
        // 请求文件列表
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withMaxKeys(fileAmountForCompress)
                .withPrefix(prefix);
        ObjectListing objectListing = s3Service.listObjects(listObjectsRequest);
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        if (objectSummaries.size() != fileAmountForCompress) {
            return;
        }

        // 批量下载文件
        // 获取下载nas的磁盘工作目录
        File workDir = new File(System.getenv("work_dir"), prefix);
        File compressFolder = new File(workDir, IdUtil.nanoId());
        if (!compressFolder.exists()) {
            compressFolder.mkdirs();
        }
        for (S3ObjectSummary objectSummary : objectSummaries) {
            String key = objectSummary.getKey();
            File file = new File(compressFolder, key);
            String url = s3Service.generatePresignedUrl(key, Duration.ofHours(1), HttpMethod.GET);
            HttpUtil.downloadFile(url, file);
        }

        // 组装zip清单文件
        String zipId = IdUtil.getSnowflake().nextIdStr();
        JSONObject manifest = new JSONObject();
        manifest.put("compressVersion", config.getString("compressVersion"));
        manifest.put("invokeId", InvokeUtil.getInvokeId());
        manifest.put("provider", "aliyun-fc");
        manifest.put("createTime", Instant.now().toString());
        manifest.put("compressId", zipId);

        JSONArray fileList = new JSONArray();
        for (S3ObjectSummary objectSummary : objectSummaries) {
            JSONObject each = new JSONObject();
            each.put("key", objectSummary.getKey());
            each.put("size", objectSummary.getSize());
            each.put("eTag", objectSummary.getETag());
            each.put("lastModified", objectSummary.getLastModified());
            fileList.add(each);
        }
        manifest.put("fileList", fileList);

        // 写入zip清单文件
        File manifestFile = new File(compressFolder, zipId + ".manifest");
        FileUtil.writeString(manifest.toJSONString(), manifestFile, StandardCharsets.UTF_8);

        //压缩
        File zipFile = new File(workDir, zipId + ".zip");
        System.out.println("压缩文件，源文件夹：" + compressFolder.getAbsolutePath());
        System.out.println("压缩文件，目标文件：" + zipFile.getAbsolutePath());
        ZipUtil.zip(compressFolder.getAbsolutePath(), zipFile.getAbsolutePath());

        // 上传zip到对象存储，直接把类型设为低频
        PutObjectRequest putObjectRequest = new PutObjectRequest(
                s3Service.getBucketName(), prefix + "/archive/", zipFile);
        putObjectRequest.withStorageClass(StorageClass.OneZoneInfrequentAccess);
        s3Service.putObject(putObjectRequest);

        // 删除对象存储data文件
        List<String> keys = new ArrayList<>();
        for (S3ObjectSummary objectSummary : objectSummaries) {
            keys.add(objectSummary.getKey());
        }
//        s3Service.deleteObjects(keys);

        // 删除本地文件
        boolean del = FileUtil.del(workDir);
        System.out.println("删除NAS文件结果：" + del);
    }

    public void run() {
        JSONObject config = getConfig();
        JSONArray pathList = config.getJSONArray("pathList");
        for (int i = 0; i < pathList.size(); i++) {
            JSONObject each = pathList.getJSONObject(i);
            String prefix = each.getString("prefix");
            Integer fileAmountForCompress = each.getInteger("fileAmountForCompress");
            handleEachPath(config, prefix, fileAmountForCompress);
        }
    }

}
