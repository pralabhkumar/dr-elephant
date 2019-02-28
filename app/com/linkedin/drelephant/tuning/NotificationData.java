package com.linkedin.drelephant.tuning;

import java.util.ArrayList;
import java.util.List;


public class NotificationData {
  enum MessagePriority {LOW, HIGH}

  private String subject;
  private List<String> content;
  private String senderAddress;
  private MessagePriority _messagePriority;

  private String notificationType;

  public NotificationData(String senderAddress) {
    this.senderAddress = senderAddress;
    content = new ArrayList<String>();
    _messagePriority = MessagePriority.LOW;
  }

  public void addContent(String data) {
    this.content.add(data);
  }

  public String getSenderAddress() {
    return this.senderAddress;
  }

  public List<String> getContent() {
    return this.content;
  }

  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public MessagePriority getMessagePriority() {
    return _messagePriority;
  }

  public void setMessagePriority(MessagePriority messagePriority) {
    _messagePriority = messagePriority;
  }

  public String getNotificationType() {
    return notificationType;
  }

  public void setNotificationType(String notificationType) {
    this.notificationType = notificationType;
  }
}
