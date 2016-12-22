<div class="col-md-12">
  <div class="table-responsive">
    <table class="table table-bordered analysis-table">
      <thead>
      <tr>
        <th></th>
        {{#each this.subDimensionContributionDetails.timeBucketsCurrent as |timestamp timeIndex|}}
        <th>{{displayMonthDayHour timestamp}}</th>
        {{/each}}
      </tr>
      </thead>
      <tbody>
      {{#each this.subDimensionContributionDetails.percentageChange as |percentageChangeArr keyIndex|}}
      <tr>
        <td>{{keyIndex}}</td>
        {{#each percentageChangeArr as |percentageChange idx|}}
        <td style="background-color: {{computeColor percentageChange}};color: {{computeTextColor percentageChange}};" id="{{keyIndex}}-{{idx}}">{{percentageChange}}%</td>
        {{/each}}
      </tr>
      {{/each}}
      </tbody>
    </table>
  </div>
</div>
