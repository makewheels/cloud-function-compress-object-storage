package com.eg.compress;

import com.github.makewheels.s3util.S3Config;
import com.github.makewheels.s3util.S3Service;

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

    public void run() {

    }

}
