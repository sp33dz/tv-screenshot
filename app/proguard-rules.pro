# Trading Replay App ProGuard Rules
-keepclassmembers class com.tradingview.replay.MainActivity$AndroidBridge {
    @android.webkit.JavascriptInterface <methods>;
}
-keepattributes JavascriptInterface
