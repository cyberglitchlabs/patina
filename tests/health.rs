use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use patina::http::{AppState, router};
use tower::ServiceExt; // provides `oneshot`, which sends a single request

#[tokio::test]
async fn healthz_returns_200_ok_without_auth() {
    let app = router(AppState::default());

    let response = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"ok");
}
