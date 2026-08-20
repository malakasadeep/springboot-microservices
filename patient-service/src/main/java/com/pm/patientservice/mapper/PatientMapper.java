package com.pm.patientservice.mapper;

import com.pm.patientservice.dto.PatientResponceDTO;
import com.pm.patientservice.model.Patient;

public class PatientMapper {
    public static PatientResponceDTO toDto(Patient patient) {
        PatientResponceDTO patientDTO = new PatientResponceDTO();
        patientDTO.setId(patient.getId().toString());
        patientDTO.setName(patient.getName());
        patientDTO.setEmail(patient.getEmail());
        patientDTO.setAddress(patient.getAddress());
        patientDTO.setDateOfBirth(patient.getDateOfBirth().toString());
        patientDTO.setRegisteredDate(patient.getRegisteredDate().toString());
        return patientDTO;
    }
}
