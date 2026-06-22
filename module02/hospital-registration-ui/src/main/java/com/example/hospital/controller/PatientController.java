package com.example.hospital.controller;

import com.example.hospital.model.PatientEvent;
import com.example.hospital.producer.PatientProducer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
public class PatientController {

    private final PatientProducer patientProducer;

    public PatientController(PatientProducer patientProducer) {
        this.patientProducer = patientProducer;
    }

    @GetMapping("/")
    public String showForm() {
        return "register";
    }

    @PostMapping("/register")
    public String registerPatient(
            @RequestParam String patientName,
            @RequestParam String department,
            Model model) {

        String patientId = "PAT-" + UUID.randomUUID().toString().substring(0, 5);

        PatientEvent event = new PatientEvent(
                patientId,
                patientName,
                department,
                "PATIENT_REGISTERED"
        );

        patientProducer.sendPatientEvent(event);

        model.addAttribute("message", "Patient registered and event published successfully.");
        model.addAttribute("patientId", patientId);

        return "register";
    }
}