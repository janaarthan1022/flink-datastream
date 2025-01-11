package com.janaa.flink_datastream.utils;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class AgeCalculator implements Serializable{

	public int getAge(String dobInput) {
		int ageInt = 0;
		try {
			// Parse the input date
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			LocalDate birthDate = LocalDate.parse(dobInput, formatter);
			// Get the current date
			LocalDate currentDate = LocalDate.now();

			// Calculate the age
			if (birthDate.isAfter(currentDate)) {
				System.out.println("The date of birth cannot be in the future.");
			} else {
				Period age = Period.between(birthDate, currentDate);
				ageInt = age.getYears();
				System.out.println("You are " + ageInt + " years old.");
			}
		} catch (DateTimeParseException e) {
			System.out.println("Invalid date format. Please enter the date in YYYY-MM-dd format.");
		}
		return ageInt;
	}
}
