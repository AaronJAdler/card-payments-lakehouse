-- Column masks and a row filter, applied by `lakehouse apply-governance` after the first batch.
-- {catalog} is replaced with the target catalog. Statements are separated by blank lines.
--
-- The job's service principal must belong to pii_readers. Otherwise the SCD2 merge would read
-- masked values, see a change on every customer, and create a false version for each of them.

CREATE OR REPLACE FUNCTION {catalog}.silver.mask_ssn(ssn STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN ssn
            ELSE concat('***-**-', right(ssn, 4)) END

CREATE OR REPLACE FUNCTION {catalog}.silver.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
            ELSE concat(left(email, 1), '***@', split_part(email, '@', 2)) END

CREATE OR REPLACE FUNCTION {catalog}.silver.mask_name(full_name STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN full_name
            ELSE concat(left(full_name, 1), '.') END

ALTER TABLE {catalog}.silver.customers ALTER COLUMN ssn SET MASK {catalog}.silver.mask_ssn

ALTER TABLE {catalog}.silver.customers ALTER COLUMN email SET MASK {catalog}.silver.mask_email

ALTER TABLE {catalog}.silver.customers ALTER COLUMN full_name SET MASK {catalog}.silver.mask_name

-- Regional analysts see only their customers' states, for example group region_ca for CA.
CREATE OR REPLACE FUNCTION {catalog}.gold.state_filter(customer_state STRING)
RETURN is_account_group_member('payments_engineers')
    OR is_account_group_member('payments_analysts_all_regions')
    OR is_account_group_member(concat('region_', lower(customer_state)))

ALTER TABLE {catalog}.gold.fct_transactions SET ROW FILTER {catalog}.gold.state_filter ON (customer_state)
