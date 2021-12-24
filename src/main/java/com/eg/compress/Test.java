package com.eg.compress;

import com.alibaba.fastjson.JSON;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.makewheels.s3util.S3Config;
import com.github.makewheels.s3util.S3Service;

import java.util.List;

public class Test {
    public static void main(String[] args) {
        S3Config s3Config = new S3Config();
        s3Config.setEndpoint("oss-cn-hongkong.aliyuncs.com");
        s3Config.setRegion("cn-hongkong");
        s3Config.setBucketName("spider-hongkong");
        s3Config.setAccessKey("LTAI5tPBYe47wSKExKA9Za3y");
        s3Config.setSecretKey("YTNRvO8ThNHFQeE6P76adcS7CTHqm1");
        S3Service s3Service = new S3Service();
        s3Service.init(s3Config);

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withMaxKeys(2)
                .withBucketName("spider-hongkong")
                .withPrefix("spider/huobi");
        ObjectListing objectListing = s3Service.listObjects(listObjectsRequest);
        System.out.println("getNextMarker " + objectListing.getNextMarker());
        System.out.println();
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        System.out.println(JSON.toJSONString(objectSummaries));
        for (S3ObjectSummary objectSummary : objectSummaries) {
            String key = objectSummary.getKey();
            System.out.println(key + " " + objectSummary.getStorageClass());
        }
    }
}
