{% docs my_model_doc %}
Alt text about the model
{% enddocs %}

{% docs my_model_doc__id %}
The user ID number with alternative text
{% enddocs %}

The following doc is never used, which should be fine.
{% docs my_model_doc__first_name %}
The user's first name - don't show this text!
{% enddocs %}

This doc is referenced by its full name
{% docs my_model_doc__last_name %}
The user's last name in this other file
{% enddocs %}
