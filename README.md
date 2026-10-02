# A Malaysian Bird Monitoring Case Study Using Raspberry Pi

This project was developed as part of the VeCAD Elite Internship 2026 at the VeCAD Research Lab, Faculty of Electrical Engineering, Universiti Teknologi Malaysia (UTM).

**Internship Period:** 1 September – 25 September 2026  
**Supervisors:** PM Muhammad Mun'im / Dr Ismahani

## Project Overview

This project implements a Raspberry Pi-based bird monitoring system for local bird sound detection and cloud-based monitoring.

The Raspberry Pi continuously captures audio through a USB microphone and performs local bird sound analysis using BirdNET. Detection results are first stored in a local SQLite database before being published through MQTT to a Firebase Firestore backend.

The local SQLite buffer allows detection records to be retained on the Raspberry Pi when cloud communication is temporarily unavailable. Once communication is restored, pending records can be published and acknowledged by the cloud bridge.

## System Architecture

```text
USB Microphone
      │
      ▼
Raspberry Pi
      │
      ▼
BirdNET Local Detection
      │
      ▼
SQLite Local Buffer
      │
      ▼
MQTT Publisher
      │
      ▼
MQTT / Firestore Bridge
      │
      ▼
Firebase Firestore
      │
      ▼
Monitoring Dashboard
