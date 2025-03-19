# mlops-forecasting-model-architecture

Se propone un infraestructura para entrenar modelos de Aprendizaje Automático

```mermaid
flowchart LR
    A(Postgres BD) -->|Datos| B(Entrenamiento)
    B --> C{Mejora de métrica}
    C -->|Si| D(Push)
    C -->|No| E(Fin)
```

## Componentes

- __Orquestador__: <a href="https://airflow.apache.org/" target="_blank"><img alt="Apache Airflow" src="https://img.shields.io/badge/-Apache Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white" height="20"/></a>

- __Plataforma de Aprendizaje Automático__: <a href="https://mlflow.org/" target="_blank"><img alt="MLflow" src="https://img.shields.io/badge/-MLflow-0194E2?style=flat-square&logo=mlflow&logoColor=white" height="20"/></a>
