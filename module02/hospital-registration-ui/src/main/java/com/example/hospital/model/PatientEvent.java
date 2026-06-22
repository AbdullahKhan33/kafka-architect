package com.example.hospital.model;

public class PatientEvent {

    private String patientId;
    private String patientName;
    private String department;
    private String eventType;

    public PatientEvent() {
    }

    public PatientEvent(String patientId, String patientName, String department, String eventType) {
        this.patientId = patientId;
        this.patientName = patientName;
        this.department = department;
        this.eventType = eventType;
    }

    public String getPatientId() {
        return patientId;
    }

    public String getPatientName() {
        return patientName;
    }

    public String getDepartment() {
        return department;
    }

    public String getEventType() {
        return eventType;
    }
}