package com.eg.compress;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.FunctionParam;

import java.io.InputStream;
import java.io.OutputStream;

public class InvokeUtil {
    private static final String invokeId = IdUtil.getSnowflake().nextIdStr();
    private static InputStream input;
    private static OutputStream output;
    private static Context context;

    public static String getInvokeId() {
        return invokeId;
    }

    public static InputStream getInput() {
        return input;
    }

    public static OutputStream getOutput() {
        return output;
    }

    public static Context getContext() {
        return context;
    }

    public static void init(InputStream input, OutputStream output, Context context) {
        InvokeUtil.input = input;
        InvokeUtil.output = output;
        InvokeUtil.context = context;
    }

    public static JSONObject getProviderParams() {
        JSONObject providerParams = new JSONObject();
        Context context = InvokeUtil.getContext();
        String requestId = context.getRequestId();
        FunctionParam functionParam = context.getFunctionParam();
        providerParams.put("requestId", requestId);
        providerParams.put("functionParam", functionParam);
        return providerParams;
    }
}
