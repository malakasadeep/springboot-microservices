package com.pm.patientservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.pm.patientservice.dto.PatientRequestDTO;
import com.pm.patientservice.dto.PatientResponseDTO;
import com.pm.patientservice.exception.EmailAlreadyExistsException;
import com.pm.patientservice.exception.PatientNotFoundException;
import com.pm.patientservice.grpc.BillingServiceGrpcClient;
import com.pm.patientservice.kafka.KafkaProducer;
import com.pm.patientservice.model.Patient;
import com.pm.patientservice.repository.PatientRepository;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PatientServiceTest {

    @Mock
    private PatientRepository patientRepository;

    @Mock
    private BillingServiceGrpcClient billingServiceGrpcClient;

    @Mock
    private KafkaProducer kafkaProducer;

    @InjectMocks
    private PatientService patientService;

    @Captor
    private ArgumentCaptor<Patient> patientCaptor;

    @Test
    void createPatient_shouldThrowEmailAlreadyExists_whenEmailIsTaken() {
        PatientRequestDTO request = requestDTO();
        when(patientRepository.existsByEmail("jane.doe@example.com")).thenReturn(
                true);

        assertThatThrownBy(() -> patientService.createPatient(request))
                .isInstanceOf(EmailAlreadyExistsException.class)
                .hasMessageContaining("jane.doe@example.com");

        verify(patientRepository, never()).save(any(Patient.class));
        verifyNoInteractions(billingServiceGrpcClient, kafkaProducer);
    }

    @Test
    void updatePatient_shouldThrowPatientNotFound_whenIdDoesNotExist() {
        UUID id = UUID.randomUUID();
        PatientRequestDTO request = requestDTO();
        when(patientRepository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> patientService.updatePatient(id, request))
                .isInstanceOf(PatientNotFoundException.class)
                .hasMessageContaining(id.toString());

        verify(patientRepository, never()).existsByEmailAndIdNot(anyString(),
                any(UUID.class));
        verify(patientRepository, never()).save(any(Patient.class));
    }

    @Test
    void createPatient_shouldMapRequestToSavedPatientAndReturnDTO() {
        PatientRequestDTO request = requestDTO();
        UUID savedId = UUID.randomUUID();

        when(patientRepository.existsByEmail("jane.doe@example.com")).thenReturn(
                false);
        when(patientRepository.save(any(Patient.class))).thenAnswer(invocation -> {
            Patient toSave = invocation.getArgument(0);
            toSave.setId(savedId);
            return toSave;
        });

        PatientResponseDTO response = patientService.createPatient(request);

        verify(patientRepository).save(patientCaptor.capture());
        Patient saved = patientCaptor.getValue();
        assertThat(saved.getId()).isEqualTo(savedId);
        assertThat(saved.getName()).isEqualTo("Jane Doe");
        assertThat(saved.getEmail()).isEqualTo("jane.doe@example.com");
        assertThat(saved.getAddress()).isEqualTo("12 Main Street, Colombo");
        assertThat(saved.getDateOfBirth()).isEqualTo(LocalDate.of(1995, 4, 12));
        assertThat(saved.getRegisteredDate()).isEqualTo(LocalDate.of(2024, 1, 20));

        assertThat(response.getId()).isEqualTo(savedId.toString());
        assertThat(response.getName()).isEqualTo("Jane Doe");
        assertThat(response.getEmail()).isEqualTo("jane.doe@example.com");
        assertThat(response.getAddress()).isEqualTo("12 Main Street, Colombo");
        assertThat(response.getDateOfBirth()).isEqualTo("1995-04-12");

        verify(billingServiceGrpcClient).createBillingAccount(savedId.toString(),
                "Jane Doe", "jane.doe@example.com");
        verify(kafkaProducer).sendEvent(saved);
    }

    private PatientRequestDTO requestDTO() {
        PatientRequestDTO request = new PatientRequestDTO();
        request.setName("Jane Doe");
        request.setEmail("jane.doe@example.com");
        request.setAddress("12 Main Street, Colombo");
        request.setDateOfBirth("1995-04-12");
        request.setRegisteredDate("2024-01-20");
        return request;
    }
}
