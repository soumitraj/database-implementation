################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../lex.yy.o 

C_SRCS += \
../lex.yy.c \
../y.tab.c 

CC_SRCS += \
../BigQ.cc \
../Comparison.cc \
../ComparisonEngine.cc \
../DBFile.cc \
../File.cc \
../Pipe.cc \
../Record.cc \
../Schema.cc \
../TwoWayList.cc \
../test.cc 

OBJS += \
./BigQ.o \
./Comparison.o \
./ComparisonEngine.o \
./DBFile.o \
./File.o \
./Pipe.o \
./Record.o \
./Schema.o \
./TwoWayList.o \
./lex.yy.o \
./test.o \
./y.tab.o 

C_DEPS += \
./lex.yy.d \
./y.tab.d 

CC_DEPS += \
./BigQ.d \
./Comparison.d \
./ComparisonEngine.d \
./DBFile.d \
./File.d \
./Pipe.d \
./Record.d \
./Schema.d \
./TwoWayList.d \
./test.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '

%.o: ../%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


