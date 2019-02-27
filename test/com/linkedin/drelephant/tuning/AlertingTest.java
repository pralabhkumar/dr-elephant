package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.alerting.EmailNotificationManager;
import java.util.List;
import models.JobSuggestedParamSet;
import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;

public class AlertingTest implements Runnable  {
  private void populateTestData() {
    try {
      initParamGenerater();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  @Override
  public void run() {
    populateTestData();
    testAlerting();
  }

  private void testAlerting(){
    NotificationManager manager = new EmailNotificationManager();
    List<NotificationData> notification = manager.generateNotificationData("test");
    boolean emailStatus = manager.sendNotification(notification);
    assertTrue(" Email Send status  " ,emailStatus);
  }


}
