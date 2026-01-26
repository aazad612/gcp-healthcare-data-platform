module.exports = {
  tables: [
    {
      name: "silver_conditions",
      bronze_ref: "conditions",
      columns: ["start", "stop", "patient_id"],
      whitespace_check: true
    },
    {
      name: "silver_patients",
      bronze_ref: "patients",
      columns: ["patient_id", "gender"],
      whitespace_check: true
    }
  ]
};