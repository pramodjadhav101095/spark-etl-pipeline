FROM eclipse-temurin:17-jdk
WORKDIR /app

COPY target/spark-demo-0.0.1-SNAPSHOT-shaded.jar app.jar

EXPOSE 7878

# JVM args to fix Spark + Java 17 module access issues (including Kryo/SerializedLambda)
ENTRYPOINT ["java", \
  "--add-opens=java.base/java.lang=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
  "--add-opens=java.base/java.nio=ALL-UNNAMED", \
  "--add-opens=java.base/java.util=ALL-UNNAMED", \
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", \
  "--add-opens=java.management/sun.management=ALL-UNNAMED", \
  "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED", \
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED", \
  "--add-exports=java.base/sun.nio.cs=ALL-UNNAMED", \
  "--add-exports=java.base/sun.security.action=ALL-UNNAMED", \
  "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED", \
  "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", \
  "-jar", "app.jar"]