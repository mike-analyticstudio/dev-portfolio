
Key engineering principles applied:

- ACID-compliant Delta tables
- Incremental processing
- Idempotent pipeline design
- Surrogate key management
- Partition-aware optimization
- Historical data preservation (SCD Type 2)

---

# 📊 Business Domains Modeled

To simulate realistic enterprise environments, multiple domains are implemented:

## 🏠 Housing Analytics
- Property sales fact modeling
- Regional performance analysis
- Historical attribute tracking (SCD Type 2)

## 🚚 Logistics & Supply Chain
- Order-to-delivery lifecycle
- Shipment tracking & SLA performance
- Event-driven status transitions

## 📉 Customer Churn
- Subscription lifecycle modeling
- Retention & churn metrics
- Customer dimension SCD implementation

## 💰 Finance & Transactions
- Revenue & expense tracking
- Snapshot vs transactional modeling
- Period-based aggregation strategies

---

# 🔄 Advanced Data Engineering Patterns Demonstrated

### Slowly Changing Dimensions (SCD)
- Type 1 and Type 2 implementations
- Expire + Insert merge strategy
- Surrogate key generation
- Effective start/end dating
- Current record flag logic

### Delta Lake MERGE Patterns
- Conditional updates
- Change detection logic
- Safe upserts
- Constraint handling

### SCD Demo: To demonstrate use test with the 3 notebooks below utilizing the notebooks scripts
#### Generate customer staging data that is used to test changes in dimension
- 1 Generate_Synthetic_CustomerNotebook.ipynb

#### Trigger changes to customer staging data where SCD process would run against the dimension
- 2 SCD_Test_Modify_Stating_CustomerNotebook.ipynb

#### This is where the implementation of the SCD occurs
- 3 Slowly Changing Dim Notebook.ipynb


### Dynamic Data Generation
- Configurable synthetic data creation
- Deterministic and randomized attributes
- Incremental load simulation

### Date Dimension Generator
- Fiscal calendar support
- ISO week calculations
- Full star-schema-ready structure

---

# 🔁 Pipeline Orchestration

Fabric pipelines coordinate notebook execution across layers:

- Bronze ingestion
- Silver transformation
- Gold dimensional modeling
- Parameterized execution
- Error handling & dependency chaining

Demonstrates end-to-end data flow automation within Microsoft Fabric.

---

# 🎯 What This Portfolio Demonstrates

This repository highlights hands-on capability in:

- Designing scalable Lakehouse architectures
- Implementing enterprise-grade dimensional models
- Writing production-quality Spark transformations
- Managing historical data correctly (SCD Type 2)
- Optimizing Delta Lake performance
- Building structured, reusable notebook workflows
- Simulating realistic multi-domain business data

---

# 📈 Why This Matters

Modern data platforms require engineers who can:

- Combine architecture thinking with implementation skill
- Design for scalability and maintainability
- Handle historical data correctly
- Build reliable, repeatable pipelines
- Bridge analytics and engineering

This project reflects those competencies in a practical, hands-on format.

---

# 🚀 How to Run

1. Import notebooks into Microsoft Fabric
2. Attach to a Lakehouse
3. Execute data generation notebooks
4. Run transformation layers
5. Trigger Fabric pipelines
6. Query Gold layer for analytics

---

# 🔮 Future Enhancements

- Streaming ingestion examples
- Real-time incremental pipelines
- Data quality framework
- Observability & monitoring layer
- Power BI semantic model integration
- CI/CD deployment patterns

---

# 👤 About This Project

This portfolio was built to demonstrate applied expertise in:

- Spark Data Engineering
- Lakehouse Architecture
- Dimensional Modeling
- Microsoft Fabric Ecosystem
- Enterprise Data Patterns

Michael Obideyi



