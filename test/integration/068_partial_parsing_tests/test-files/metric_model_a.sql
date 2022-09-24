{%
    set metric_list = [
        metric('number_of_people'),
        metric('collective_tenure')
    ]
%}

{% if not execute %}                                
        
    {% set metric_names = [] %}                                         
    {% for m in metric_list %}             
        {% do metric_names.append(m.metric_name) %}           
    {% endfor %}                                    
                                 
    -- this config does nothing, but it lets us check these values
    {{ config(metric_names = metric_names) }}       
                             
{% endif %}
 

select 1 as fun
