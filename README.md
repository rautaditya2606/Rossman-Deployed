# Rossmann Store Sales Predictor

---
### 🔗 **[Live Demo](https://rossman-deployed-xxk0.onrender.com/)**
---

A machine learning web application that predicts daily sales for Rossmann stores using XGBoost. This application helps store managers make data-driven decisions by forecasting future sales based on various store attributes and temporal features.

## Features

- **Sales Prediction**: Predict daily sales for any Rossmann store
- **Web Interface**: User-friendly interface for easy interaction
- **REST API**: Programmatic access to the prediction model
- **Store Metadata Integration**: Automatically incorporates store-specific features
-   **MLOps Pipeline**: Real-time logging of inputs and predictions to PostgreSQL
-   **Automated Data Generation**: Background scheduler that logs synthetic traffic for monitoring
-   **Cloud Native**: Deployed on Render with PostgreSQL integration

## Prerequisites

-   Python 3.9+
-   PostgreSQL (Local or managed like Render)
-   pip (Python package manager)

## Database Configuration

The application logs all predictions to a PostgreSQL database.

### Table Schema: `rossman_deployed`
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | SERIAL | Primary Key |
| `timestamp` | TIMESTAMP | Time of prediction |
| `store_id` | INTEGER | Rossmann Store ID |
| `date` | DATE | Sale date for prediction |
| `promo` | INTEGER | Promotion active (0/1) |
| `state_holiday` | VARCHAR | Holiday type ('0', 'a', 'b', 'c') |
| `school_holiday` | INTEGER | School holiday active (0/1) |
| `prediction` | DOUBLE | Forecasted Sales value |
| `data_source` | VARCHAR | 'user' (web/API) or 'synthetic' (auto-generated) |

### Setup Instructions

1.  **Environment Variables**: Create a `.env` file in the root directory:
    ```env
    DATABASE_URL=your_postgresql_url
    TABLE_NAME=rossman_deployed
    ```
    *Note: For Render, use the **Internal URL** for the web service and the **External URL** for local testing.*

2.  **Initialize Database**:
    ```bash
    python init_db.py
    ```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/rautaditya2606/Rossman-Deployed.git
   cd Rossman-Deployed
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Web Interface

1. Run the Flask application:
   ```bash
   python app.py
   ```
2. Open your browser and navigate to `http://localhost:5000`
3. Fill in the store details and submit to get sales predictions

### API Endpoint

You can also make predictions programmatically using the API endpoint:

```bash
curl -X POST "http://your-server-address/api/predictor" \
     -H "Content-Type: application/json" \
     -d '{
           "Store": 1,
           "Date": "2023-12-31",
           "Promo": 1,
           "StateHoliday": "0",
           "SchoolHoliday": 0
         }'
```

**Example Response:**
```json
{
  "prediction": 5263.45
}
```

## Model Details

The prediction model is built using XGBoost and considers the following features:

### Numerical Features
- Store ID
- Promo status (0 or 1)
- School holiday status (0 or 1)
- Competition distance
- Days since competition opened
- Promo2 status (0 or 1)
- Days since Promo2 started
- Is it a Promo2 month? (0 or 1)
- Day of month
- Month
- Year
- Week of year

### Categorical Features
- Day of week
- State holiday type
- Store type
- Assortment type

## Project Structure

```
Rossman-Deployed/
├── app.py                # Flask application
├── requirements.txt      # Python dependencies
├── README.md             # This file
├── model/                # Trained model and encoders
│   ├── xgb_pipeline.joblib
│   ├── scaler.joblib
│   ├── encoder.joblib
│   └── store_static_dict.joblib
├── static/               # Static files (CSS, JS, images)
│   ├── css/
│   └── js/
├── templates/            # HTML templates
│   └── index.html
└── utils.py             # Helper functions
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Rossmann Store Sales](https://www.kaggle.com/c/rossmann-store-sales) - Kaggle competition
- [XGBoost](https://xgboost.readthedocs.io/) - Gradient boosting library
- [Flask](https://flask.palletsprojects.com/) - Web framework

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Contact

For any questions or feedback, please open an issue on GitHub.
