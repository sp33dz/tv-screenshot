package com.tradingview.replay;

import android.annotation.SuppressLint;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.view.View;
import android.view.WindowManager;
import android.webkit.CookieManager;
import android.webkit.JavascriptInterface;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceRequest;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Toast;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.view.WindowCompat;
import androidx.core.view.WindowInsetsCompat;
import androidx.core.view.WindowInsetsControllerCompat;

public class MainActivity extends AppCompatActivity {

    private WebView webView;
    private boolean isFullscreen = false;
    private WindowInsetsControllerCompat insetsController;

    @SuppressLint({"SetJavaScriptEnabled", "JavascriptInterface"})
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // Edge-to-edge
        WindowCompat.setDecorFitsSystemWindows(getWindow(), false);
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);

        setContentView(R.layout.activity_main);

        webView = findViewById(R.id.webview);
        insetsController = WindowCompat.getInsetsController(getWindow(), getWindow().getDecorView());

        setupWebView();
        webView.loadUrl("file:///android_asset/replay.html");
    }

    @SuppressLint("SetJavaScriptEnabled")
    private void setupWebView() {
        WebSettings settings = webView.getSettings();

        // Enable JavaScript
        settings.setJavaScriptEnabled(true);

        // DOM Storage (for localStorage / sessionStorage)
        settings.setDomStorageEnabled(true);

        // Cache & network
        settings.setCacheMode(WebSettings.LOAD_DEFAULT);
        settings.setDatabaseEnabled(true);

        // Media / image loading
        settings.setLoadsImagesAutomatically(true);
        settings.setMixedContentMode(WebSettings.MIXED_CONTENT_ALWAYS_ALLOW);

        // Zoom
        settings.setSupportZoom(false);  // We handle zoom in JS/touch
        settings.setBuiltInZoomControls(false);

        // Layout
        settings.setUseWideViewPort(true);
        settings.setLoadWithOverviewMode(true);

        // Hardware acceleration
        webView.setLayerType(View.LAYER_TYPE_HARDWARE, null);

        // Hide scrollbars
        webView.setScrollBarStyle(View.SCROLLBARS_INSIDE_OVERLAY);
        webView.setScrollbarFadingEnabled(true);
        webView.setOverScrollMode(View.OVER_SCROLL_NEVER);

        // Enable third-party cookies (required for Google Drive / lh3.googleusercontent.com)
        CookieManager cookieManager = CookieManager.getInstance();
        cookieManager.setAcceptCookie(true);
        cookieManager.setAcceptThirdPartyCookies(webView, true);

        // Set User-Agent ให้ดูเหมือน Chrome browser เพื่อให้ Google serve รูปภาพได้
        String defaultUA = settings.getUserAgentString();
        settings.setUserAgentString(defaultUA.replace("wv", "") + " Chrome/120.0.0.0");

        // JavaScript bridge
        webView.addJavascriptInterface(new AndroidBridge(), "AndroidBridge");

        // WebViewClient
        webView.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                String url = request.getUrl().toString();
                // Allow all URLs (data.json, images from GitHub/Drive)
                return false;
            }

            @Override
            public void onPageFinished(WebView view, String url) {
                super.onPageFinished(view, url);
            }
        });

        // WebChromeClient for console logs and fullscreen
        webView.setWebChromeClient(new WebChromeClient() {
            @Override
            public void onShowCustomView(View view, CustomViewCallback callback) {
                // Handle fullscreen video/media
                super.onShowCustomView(view, callback);
            }

            @Override
            public void onHideCustomView() {
                super.onHideCustomView();
            }
        });
    }

    @Override
    public void onBackPressed() {
        // Ask JS to handle back press first
        webView.evaluateJavascript(
            "(function(){ return window.onBackPressed ? window.onBackPressed() : false; })()",
            result -> {
                if (!"true".equals(result)) {
                    super.onBackPressed();
                }
            }
        );
    }

    /* ── Android Bridge — called from JavaScript ── */
    private class AndroidBridge {

        @JavascriptInterface
        public String getNotes() {
            SharedPreferences prefs = getSharedPreferences("TradingReplay", MODE_PRIVATE);
            return prefs.getString("notes", "{}");
        }

        @JavascriptInterface
        public void saveNotes(String notesJson) {
            SharedPreferences prefs = getSharedPreferences("TradingReplay", MODE_PRIVATE);
            prefs.edit().putString("notes", notesJson).apply();
        }

        @JavascriptInterface
        public void toggleFullscreen() {
            runOnUiThread(() -> {
                isFullscreen = !isFullscreen;
                if (isFullscreen) {
                    insetsController.hide(WindowInsetsCompat.Type.systemBars());
                    insetsController.setSystemBarsBehavior(
                        WindowInsetsControllerCompat.BEHAVIOR_SHOW_TRANSIENT_BARS_BY_SWIPE);
                } else {
                    insetsController.show(WindowInsetsCompat.Type.systemBars());
                }
            });
        }

        @JavascriptInterface
        public void onFrameChange(String frameJson) {
            // Optional: could update notification, status bar, etc.
        }

        @JavascriptInterface
        public void showToast(String msg) {
            runOnUiThread(() ->
                Toast.makeText(MainActivity.this, msg, Toast.LENGTH_SHORT).show()
            );
        }

        @JavascriptInterface
        public void keepScreenOn(boolean on) {
            runOnUiThread(() -> {
                if (on) {
                    getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
                } else {
                    getWindow().clearFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
                }
            });
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        webView.onResume();
    }

    @Override
    protected void onPause() {
        super.onPause();
        webView.onPause();
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        webView.stopLoading();
        webView.destroy();
    }
}
