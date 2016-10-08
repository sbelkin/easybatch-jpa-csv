package io.sbelkin.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.sbelkin.testing.entity.Employee;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import io.sbelkin.testing.mapper.JavaObjectMapper;
import org.easybatch.core.job.Job;
import org.easybatch.core.job.JobBuilder;
import org.easybatch.core.job.JobExecutor;
import org.easybatch.core.job.JobReport;
import org.easybatch.core.mapper.BatchMapper;
import org.easybatch.core.marshaller.BatchMarshaller;
import org.easybatch.core.reader.IterableBatchReader;
import org.easybatch.core.record.StringRecord;
import org.easybatch.core.writer.FileBatchWriter;
import org.easybatch.core.writer.FileRecordWriter;
import org.easybatch.extensions.jackson.JacksonRecordMapper;
import org.easybatch.flatfile.DelimitedRecordMapper;
import org.easybatch.jdbc.JdbcRecordMapper;
import org.easybatch.jdbc.JdbcRecordReader;
import org.easybatch.flatfile.DelimitedRecordMarshaller;
import org.easybatch.jpa.JpaBatchReader;
import org.easybatch.jpa.JpaEntityManagerListener;
import org.easybatch.jpa.JpaRecordWriter;
import org.easybatch.jpa.JpaTransactionListener;

import java.beans.IntrospectionException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by sbelkin on 9/20/2016.
 */
public class JPAEntityCsv {

    public static void main(String[] args) throws Exception {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory( "Eclipselink_JPA" );
//        create(emf);
//        find(emf);
        iterable(emf);
        emf.close( );
    }

    public static void create(EntityManagerFactory emf){
        EntityManager em = emf.createEntityManager();
        em.getTransaction( ).begin( );
        Employee employee = new Employee( );
        employee.setEid( 1201 );
        employee.setEname( "Gopal" );
        employee.setSalary( 40000 );
        employee.setDeg( "Technical Manager" );
        em.merge( employee );
        em.getTransaction( ).commit( );
    }

    public static void find(EntityManagerFactory emf){
        EntityManager em = emf.createEntityManager();
        em.getTransaction( ).begin( );
        Employee employee = em.find( Employee.class, 101 );
        System.out.println("employee ID = " + employee.getEid( ));
        System.out.println("employee NAME = " + employee.getEname( ));
        System.out.println("employee SALARY = " + employee.getSalary( ));
        System.out.println("employee DESIGNATION = " + employee.getDeg( ));
        em.close();
    }

    public static void query(EntityManagerFactory emf) throws IntrospectionException, IOException {
        FileWriter employee = new FileWriter(new File("emp.csv"));
        EntityManager em = emf.createEntityManager( );
        Query q =  em.createQuery("select e from Employee e");
        String query = "select e from Employee e";
        String[] field ={"eid", "ename","salary","deg"};
        Job job = JobBuilder.aNewJob()
                .reader(new JpaBatchReader<Employee>(1,emf,query,Employee.class))
                .marshaller(new BatchMarshaller(new DelimitedRecordMarshaller(Employee.class, field)))
                .writer(new FileBatchWriter(employee))
                .pipelineListener(new JpaTransactionListener(em))
                .jobListener(new JpaEntityManagerListener(em))
                .build();
        JobReport jobReport = JobExecutor.execute(job);
    }

    public static void iterable(EntityManagerFactory emf) throws IntrospectionException, IOException {
        FileWriter employee = new FileWriter(new File("emp.csv"));
        EntityManager em = emf.createEntityManager( );
        Query q =  em.createQuery("select e from Employee e");
        String[] field ={"eid", "ename","salary","deg"};

        List list = q.getResultList();

        Job job = JobBuilder.aNewJob()
                .reader(new IterableBatchReader(list,2))
                .marshaller(new BatchMarshaller(new DelimitedRecordMarshaller(Employee.class, field)))
                .writer(new FileBatchWriter(employee))
                .build();
        JobReport jobReport = JobExecutor.execute(job);
        em.close( );
    }
}
