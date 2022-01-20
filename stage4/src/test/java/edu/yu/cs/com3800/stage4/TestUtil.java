package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.stage4.GatewayServer;

public class TestUtil {
    public final static int gatewayPort = 8888;

    public final static String HTTPENDPOINT = "/test/compileandrun";


    public final static String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    public final static String invalidClass = "edu.yu.cs.fall2019.com3800.stage1;\n\npublic HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

    public static String getVersionedValidClass(int version) {
        return "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world! from code version " + version + "\";\n    }\n}\n";
    }
}
