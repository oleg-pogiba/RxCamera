package com.ragnarok.rxcamera.request;

import com.ragnarok.rxcamera.RxCamera;
import com.ragnarok.rxcamera.RxCameraData;

import rx.Observable;

/**
 * Created by ragnarok on 15/11/15.
 * CameraRequestBuilder can help you get the camera preview frame data in different way
 */
public class RxCameraRequestBuilder {

    private RxCamera rxCamera;

    public RxCameraRequestBuilder(RxCamera rxCamera) {
        this.rxCamera = rxCamera;
    }

    /**
     * successive camera preview frame data
     * @return Observable contained the camera data
     */
    public Observable<RxCameraData> successiveDataRequest() {
        return new SuccessiveDataRequest(rxCamera).get();
    }

    /**
     * periodic camera preview frame data
     * @param intervalMills the interval of the preview frame data will return, in millseconds
     * @returni Observable contained the camera data
     */
    public Observable<RxCameraData> periodicDataRequest(long intervalMills) {
        return new PeriodicDataRequest(rxCamera, intervalMills).get();
    }

    /**
     * only one shot camera data, encapsulated the setOneShotPreviewCallback
     * @return Observable contained the camera data
     */
    public Observable<RxCameraData> oneShotRequest() {
        return new TakeOneShotRequest(rxCamera).get();
    }
}